defmodule Bypass.Instance do
  defmodule State do
    defstruct [
      expectations: %{},
      port: nil,
      ref: nil,
      callers_awaiting_down: [],
      callers_awaiting_exit: [],
      pass: false,
      unknown_route_error: nil,
      monitors: %{}
    ]

  end
  @moduledoc false

  use GenServer, restart: :transient

  import Bypass.Utils
  import Plug.Router.Utils, only: [build_path_match: 1]

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, [opts])
  end

  def call(pid, request) do
    debug_log("call(#{inspect(pid)}, #{inspect(request)})")
    result = GenServer.call(pid, request, :infinity)
    debug_log("#{inspect(pid)} -> #{inspect(result)}")
    result
  end

  def cast(pid, request) do
    GenServer.cast(pid, request)
  end

  # GenServer callbacks

  def init([opts]) do
    case do_up(Keyword.get(opts, :port, 0)) do
      {:ok, ref} ->
        port = :ranch.get_port(ref)
        state = %State{
          port: port,
          ref: ref,
        }

        {:ok, state}

      {:error, reason}  ->
        {:stop, {:up_error, reason}}
    end
  end

  def handle_info({:DOWN, ref, _, _, reason}, state) do
    case pop_in(state.monitors[ref]) do
      {nil, state} ->
        {:noreply, state}

      {route, state} ->
        result = {:exit, {:exit, reason, []}}
        route
        |> put_result(ref, result, state)
        |> dispatch_awaiting_callers()
    end
  end

  def handle_cast({:put_expect_result, route, ref, result}, state) do
    route
    |> put_result(ref, result, state)
    |> dispatch_awaiting_callers()
  end

  def handle_call(request, from, state) do
    debug_log([inspect(self()), " called ", inspect(request), " with state ", inspect(state)])
    do_handle_call(request, from, state)
  end

  defp do_handle_call(:port, _, %State{port: port} = state) do
    {:reply, port, state}
  end

  defp do_handle_call(:up, _from, %State{port: port, ref: nil} = state) do
    case do_up(port) do
      {:ok, ref} ->
        {:reply, :ok, %{state | ref: ref}}

      {:error, _reason} = err ->
        {:reply, err, state}
    end
  end

  defp do_handle_call(:up, _from, %State{} = state) do
    {:reply, {:error, :already_up}, state}
  end

  defp do_handle_call(:down, _from, %State{ref: nil} = state) do
    {:reply, {:error, :already_down}, state}
  end

  defp do_handle_call(
         :down,
         from,
         %State{ref: ref, callers_awaiting_down: callers_awaiting_down} = state
       )
       when not is_nil(ref) do
    if has_any_retained_plugs?(state) do
      # wait for plugs to finish
      {:noreply, %{state | callers_awaiting_down: [from | callers_awaiting_down]}}
    else
      :ok = do_down(ref)
      {:reply, :ok, %{state | ref: nil}}
    end
  end

  defp do_handle_call({expect, fun}, from, state) when expect in [:expect, :expect_once] do
    do_handle_call({expect, :any, :any, fun}, from, state)
  end

  defp do_handle_call(
         {expect, method, path, fun},
         _from,
         %State{expectations: expectations} = state
       )
       when expect in [:stub, :expect, :expect_once] and
              method in [
                "GET",
                "POST",
                "HEAD",
                "PUT",
                "PATCH",
                "DELETE",
                "OPTIONS",
                "CONNECT",
                :any
              ] and
              (is_binary(path) or path == :any) and
              is_function(fun, 1) do
    route = {method, path}

    updated_expectations =
      Map.put(
        expectations,
        route,
        new_route(
          fun,
          path,
          case expect do
            :expect -> :once_or_more
            :expect_once -> :once
            :stub -> :none_or_more
          end
        )
      )

    {:reply, :ok, %{state | expectations: updated_expectations}}
  end

  defp do_handle_call({expect, _, _, _}, _from, _state)
       when expect in [:expect, :expect_once] do
    raise "Route for #{expect} does not conform to specification"
  end

  defp do_handle_call({:get_route, method, path}, _from, state) do
    {route, _} = route_info(method, path, state)
    {:reply, route, state}
  end

  defp do_handle_call(:pass, _from, state) do
    updated_state =
      Enum.reduce(state.expectations, state, fn {route, route_expectations}, state_acc ->
        Enum.reduce(route_expectations.retained_plugs, state_acc, fn {ref, _}, plugs_acc ->
          put_result(route, ref, :ok, plugs_acc)
        end)
      end)

    {:reply, :ok, %{updated_state | pass: true}}
  end

  defp do_handle_call(
         {:get_expect_fun, route},
         from,
         %State{expectations: expectations} = state
       ) do
    case Map.get(expectations, route) do
      %{expected: :once, request_count: count} when count > 0 ->
        {:reply, {:error, :too_many_requests, route}, increase_route_count(state, route)}

      nil ->
        {:reply, {:error, :unexpected_request, route}, state}

      route_expectations ->
        state = increase_route_count(state, route)
        {ref, state} = retain_plug_process(route, from, state)
        {:reply, {:ok, ref, route_expectations.fun}, state}
    end
  end

  defp do_handle_call(:on_exit, from, %State{callers_awaiting_exit: callers} = state) do
    if has_any_retained_plugs?(state) do
      {:noreply, %{state | callers_awaiting_exit: [from | callers]}}
    else
      {result, updated_state} = do_exit(state)
      {:stop, :normal, result, updated_state}
    end
  end

  defp do_exit(%State{} = state) do
    updated_state =
      case state do
        %State{ref: nil} ->
          state

        %State{ref: ref} ->
          :ok = do_down(ref)
          %{state | ref: nil}
      end

    result =
      cond do
        state.pass ->
          :ok

        state.unknown_route_error ->
          state.unknown_route_error

        true ->
          case expectation_problem_message(state.expectations) do
            nil -> :ok
            error -> error
          end
      end

    {result, updated_state}
  end

  defp put_result(route, ref, result, state) do
    if state.expectations[route] do
      {_, state} = pop_in(state.monitors[ref])

      update_in(state.expectations[route], fn route_expectations ->
        plugs = route_expectations.retained_plugs

        Map.merge(route_expectations, %{
          retained_plugs: Map.delete(plugs, ref),
          results: [result | Map.fetch!(route_expectations, :results)]
        })
      end)
    else
      Map.put(state, :unknown_route_error, result)
    end
  end

  defp increase_route_count(state, route) do
    update_in(
      state.expectations[route],
      fn route_expectations -> Map.update(route_expectations, :request_count, 1, &(&1 + 1)) end
    )
  end

  defp expectation_problem_message(expectations) do
    problem_route =
      expectations
      |> Enum.reject(fn {_route, expectations} -> expectations[:expected] == :none_or_more end)
      |> Enum.find(fn {_route, expectations} -> Enum.empty?(expectations.results) end)

    case problem_route do
      {route, _} ->
        {:error, :not_called, route}

      nil ->
        Enum.reduce_while(expectations, nil, fn {_route, route_expectations}, _ ->
          first_error =
            Enum.find(route_expectations.results, fn
              result when is_tuple(result) -> result
              _result -> nil
            end)

          case first_error do
            nil -> {:cont, nil}
            error -> {:halt, error}
          end
        end)
    end
  end

  defp route_info(method, path, %State{expectations: expectations} = _state) do
    segments = build_path_match(path) |> elem(1)

    route =
      expectations
      |> Enum.reduce_while(
        {:any, :any, %{}},
        fn
          {{^method, path_pattern}, %{path_parts: path_parts}}, acc ->
            case match_route(segments, path_parts) do
              {true, params} -> {:halt, {method, path_pattern, params}}
              {false, _} -> {:cont, acc}
            end

          _, acc ->
            {:cont, acc}
        end
      )

    {route, Map.get(expectations, route)}
  end

  defp match_route(path, route) when length(path) == length(route) do
    path
    |> Enum.zip(route)
    |> Enum.reduce_while(
      {true, %{}},
      fn
        {value, {param, _, _}}, {_, params} ->
          {:cont, {true, Map.put(params, Atom.to_string(param), value)}}

        {segment, segment}, acc ->
          {:cont, acc}

        _, _ ->
          {:halt, {false, nil}}
      end
    )
  end

  defp match_route(_, _), do: {false, nil}

  @spec do_up(:inet.port_number()) :: {:ok, reference()}
  defp do_up(port) do
    plug_opts = [bypass_instance: self()]

    ref = make_ref()

    cowboy_opts = make_cowboy_opts(port, ref)
    case Plug.Cowboy.http(Bypass.Plug, plug_opts, cowboy_opts) do
      {:ok, _pid} ->
        {:ok, ref}

      {:error, _} = err ->
        err
    end
  end

  defp make_cowboy_opts(port, ref) do
    [
      ref: ref,
      port: port,
      transport_options: [num_acceptors: 5]
    ]
  end

  defp do_down(ref) when is_reference(ref) do
    case Plug.Cowboy.shutdown(ref) do
      :ok ->
        :ok

      {:error, :not_found} ->
        :ok
    end
  end

  defp retain_plug_process({method, path} = route, {caller_pid, _}, state) do
    debug_log([
      inspect(self()),
      " retain_plug_process ",
      inspect(caller_pid),
      ", retained_plugs: ",
      inspect(
        Map.get(state.expectations, route)
        |> Map.get(:retained_plugs)
        |> Map.values()
      )
    ])

    ref = Process.monitor(caller_pid)

    state =
      update_in(state.expectations[route][:retained_plugs], fn plugs ->
        Map.update(plugs, ref, caller_pid, fn _ ->
          raise "plug already installed for #{method} #{path}"
        end)
      end)

    {ref, put_in(state.monitors[ref], route)}
  end

  defp dispatch_awaiting_callers(
         %State{
           callers_awaiting_down: down_callers,
           callers_awaiting_exit: exit_callers,
           ref: ref
         } = state
       ) do
    if has_any_retained_plugs?(state) do
      {:noreply, state}
    else
      state =
        case down_callers do
          [] ->
            state

          [_ | _] = down_callers ->
            :ok = do_down(ref)
            Enum.each(down_callers, &GenServer.reply(&1, :ok))
            %{state | callers_awaiting_down: []}
        end

      case exit_callers do
        [] ->
          {:noreply, state}

        [_ | _] = exit_callers ->
          {result, _updated_state} = do_exit(state)
          Enum.each(exit_callers, &GenServer.reply(&1, result))
          {:stop, :normal, state}
      end
    end
  end

  @spec has_any_retained_plugs?(map()) :: boolean()
  defp has_any_retained_plugs?(state) do
    state.expectations
    |> Enum.any?(fn {_, %{retained_plugs: retained_plugs}} ->
      not Enum.empty?(retained_plugs)
    end)
  end

  defp new_route(fun, path_parts, expected) when is_list(path_parts) do
    %{
      fun: fun,
      expected: expected,
      path_parts: path_parts,
      retained_plugs: %{},
      results: [],
      request_count: 0
    }
  end

  defp new_route(fun, :any, expected) do
    new_route(fun, [], expected)
  end

  defp new_route(fun, path, expected) do
    new_route(fun, build_path_match(path) |> elem(1), expected)
  end
end
