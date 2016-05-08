defmodule Formless.Store.Backup do
  # Store for backing up indexed data
  # Will be used for reindexing at some point
  use GenServer

  # Client API

  def start_link(name) do
    GenServer.start_link(__MODULE__, :ok, [name: name])
  end

  def write_text(server, bucket, text) do
    GenServer.call(server, {:write_text, bucket, text})
  end

  def text_stream(server, bucket) do
    Stream.unfold 1, fn(index) ->
      case GenServer.call(server, {:get_text, bucket, index}) do
        {:ok, text} -> {text, index + 1}
        :done -> nil
      end
    end
  end

  def drop_bucket(server, bucket) do
    GenServer.call(server, {:drop_bucket, bucket})
  end

  def stop(server, reason, timeout) do
    GenServer.stop(server, reason, timeout)
  end


  # Server API

  def init(:ok) do
    data_dir = Application.get_env(:formless, Formless)[:data_dir]
    File.mkdir(data_dir)
    db_path = Path.join(data_dir, "backup.db")
    {:ok, store} = Exleveldb.open(db_path, [create_if_missing: true])

    {:ok, %{store: store}}
  end

  defp get_counters(store) do
    case Exleveldb.get(store, "counters") do
      {:ok, counters} -> :erlang.binary_to_term(counters)
      :not_found -> %{}
    end
  end

  defp get_counter(store, bucket) do
    counters = get_counters(store)
    counters[bucket] || 0
  end

  defp increment_counter(store, bucket) do
    counters = get_counters(store)
    value = (counters[bucket] || 0) + 1
    counters = Map.put(counters, bucket, value)
    :ok = Exleveldb.put(store, "counters", :erlang.term_to_binary(counters))
    value
  end

  defp clear_counter(store, bucket) do
    counters = get_counters(store)
    counters = Map.delete(counters, bucket)
    :ok = Exleveldb.put(store, "counters", :erlang.term_to_binary(counters))
  end

  def handle_call({:write_text, bucket, text}, _from, %{store: store} = state) do
    next = increment_counter(store, bucket)
    :ok = Exleveldb.put(store, "buckets.#{bucket}.#{next}", text)
    {:reply, :ok, state}
  end

  def handle_call({:get_text, bucket, index}, _from, %{store: store} = state) do
    if index > get_counter(store, bucket) do
      {:reply, :done, state}
    else
      case Exleveldb.get(store, "buckets.#{bucket}.#{index}") do
        {:ok, value} -> {:reply, {:ok, value}, state}
        :not_found -> {:reply, {:ok, nil}, state} # hopefully this never happens?
      end
    end
  end

  def handle_call({:drop_bucket, bucket}, _from, %{store: store} = state) do
    regex = Regex.compile!("buckets.#{bucket}.([0-9]+)", "u")
    store
    |> Exleveldb.stream(:keys_only)
    |> Stream.filter(&Regex.match?(regex, &1))
    |> Enum.each(&Exleveldb.delete(store, &1))
    clear_counter(store, bucket)
    {:reply, :ok, state}
  end

  def terminate(_reason, %{store: store}) do
    :ok = Exleveldb.close(store)
  end
end
