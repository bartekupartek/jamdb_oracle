defmodule OraLixir.Connection do
  @moduledoc false

  @dpiMajorVersion 3
  @dpiMinorVersion 0

  use DBConnection

  defstruct [:oranifNode, :context, :conn, :mode]

  defmacrop oranif(slave, api, args) do
    quote do
      try do
        case unquote(slave) do
          nil -> Kernel.apply(:dpi, unquote(api), unquote(args))
          _ -> :rpc.call(unquote(slave), :dpi, unquote(api), unquote(args))
        end
      rescue
        e in ErlangError ->
          {:error, file, line, original} = e.original

          {:error,
           %{
             reason: original,
             oranifFile: file,
             oranifLine: line,
             api: unquote(api),
             args: unquote(args),
             node: unquote(slave)
           }}
      end
    end
  end

  @impl true
  def checkin(s) do
    IO.inspect("checking")
    {:ok, s}
  end

  @impl true
  def checkout(s) do
    IO.inspect("checkout")

    {:ok, s}
  end

  @impl true
  def connect(opts) do
    ora = %OraLixir.Connection{mode: :idle}

    case Keyword.fetch(opts, :slave) do
      {:ok, slave} -> :dpi.load(slave)
      :error -> :dpi.load_unsafe()
    end
    |> case do
      :ok ->
        create_context_connection(ora, opts)

      slave when is_atom(slave) ->
        create_context_connection(%{ora | oranifNode: slave}, opts)

      error ->
        {:error, error}
    end
  end

  @impl true
  def disconnect(_err, ora) do
    IO.inspect(ora, label: :disconnect)
    if ora.conn != nil, do: oranif(ora.oranifNode, :conn_close, [ora.conn, [], ""])
    if ora.context != nil, do: oranif(ora.oranifNode, :context_destroy, [ora.context])
#    if ora.oranifNode != node(), do: :dpi.unload(ora.oranifNode)
    :ok
  end

  @impl true
  def handle_begin(opts, %{mode: mode} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when mode == :idle ->
        {:ok, :transaction, %{s | mode: :transaction}}
      :savepoint when mode == :transaction ->
        {:ok, :savepoint, %{s | mode: :transaction}}
      status when status in [:transaction, :savepoint] ->
        {status, s}
    end
  end


  @impl true
  def handle_close(_query, _opts, state) do
    raise "handle_close"
    {:ok, :handle_close, state}
  end

  @impl true
  def handle_commit(opts, %{oranifNode: slave, conn: conn, mode: mode} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when mode == :transaction ->
        case oranif(slave, :conn_commit, [conn]) do
          :ok -> {:ok, :ok, %{s | mode: :idle}}
          error -> {:disconnect, error, s}
        end
      :savepoint when mode == :transaction ->
        {:ok, [], %{s | mode: :transaction}}
      status when status in [:transaction, :savepoint] ->
        {status, s}
    end
  end

  @impl true
  def handle_rollback(opts, %OraLixir.Connection{conn: conn, oranifNode: slave, mode: mode} = state) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when mode in [:transaction, :error] ->
        case oranif(slave, :conn_rollback, [conn]) do
          :ok -> {:ok, :ok, state}
          error -> {:disconnect, error, state}
        end
      :savepoint when mode in [:transaction, :error] ->
        case oranif(slave, :conn_rollback, [conn]) do
          :ok -> {:ok, :ok, state}
          error -> {:disconnect, error, state}
        end
      status when status in [:transaction, :savepoint] ->
        {status, state}
    end
  end

  @impl true
  def handle_deallocate(_query, _cursor, _opts, state) do
    raise "handle_deallocate"

    {:ok, :handle_deallocate, state}
  end

  @impl true
  def handle_prepare(
        %Jamdb.Oracle.Query{query_str: queryStr} = query,
        opts,
        %OraLixir.Connection{conn: conn, oranifNode: slave} = state
      ) do

    {:ok, query, state}
  end

  @impl true
  def handle_execute(
        %Jamdb.Oracle.Query{statement: statement, query_str: query_str} = query,
        params,
        _opts,
        %OraLixir.Connection{conn: conn, oranifNode: slave} = state
      )  do
    case oranif(slave, :conn_prepareStmt, [conn, false, query_str, <<>>]) do
      statement when is_reference(statement) ->
        info = oranif(slave, :stmt_getInfo, [statement])
        vars = define_params(slave, conn, statement, params)
        case oranif(slave, :stmt_execute, [statement, []]) do
          numberOfColumns when is_integer(numberOfColumns) ->
            results = execute_query(numberOfColumns, %{query | statement: statement, info: info}, state)

            Enum.each(vars, fn v -> oranif(slave, :var_release, [v]) end)
            oranif(slave, :stmt_close, [statement, <<>>])

            results

          {:error, %{reason: %{message: message}}} = error when is_list(message) ->
            {:error, Jamdb.Oracle.Error.exception(message: List.to_string(message), query: query_str), state}
          {:error, error} ->
            {:disconnect, error}
        end

      {:error, error} ->
        {:error, oranif_error(error), state}
    end
  end

  defp define_params(slave, conn, statement, params) do
    params
    |> Enum.with_index()
    |> Enum.map(fn {param, index} -> define_param(slave, conn, statement, index, param) end)
  end

  defp define_param(slave, conn, statement, index, binary)
       when is_binary(binary) and byte_size(binary) > 4000 do
    %{var: v} =
      oranif(slave, :conn_newVar, [
        conn,
        :DPI_ORACLE_TYPE_CLOB,
        :DPI_NATIVE_TYPE_LOB,
        byte_size(binary),
        byte_size(binary),
        true,
        false,
        :null
      ])

    oranif(slave, :stmt_bindByName, [statement, ":#{index + 1}", v])
    oranif(slave, :var_setFromBytes, [v, 0, binary])

    v
  end

  defp define_param(slave, conn, statement, index, "") do
    %{var: v} =
      oranif(slave, :conn_newVar, [
        conn,
        :DPI_ORACLE_TYPE_CLOB,
        :DPI_NATIVE_TYPE_LOB,
        1,
        0,
        false,
        false,
        :null
      ])

    oranif(slave, :stmt_bindByName, [statement, ":#{index + 1}", v])
    oranif(slave, :var_setFromBytes, [v, 0, :null])

    v
  end

  defp define_param(slave, conn, statement, index, binary) when is_binary(binary) do
    if String.valid?(binary) do
      %{var: v} = oranif(slave, :conn_newVar, [
        conn,
        :DPI_ORACLE_TYPE_VARCHAR,
        :DPI_NATIVE_TYPE_BYTES,
        byte_size(binary),
        byte_size(binary),
        true,
        false,
        :null
      ])
      oranif(slave, :stmt_bindByName, [statement, ":#{index + 1}", v])
      oranif(slave, :var_setFromBytes, [v, 0, binary])

      v
    else
      %{var: v} = oranif(slave, :conn_newVar, [
        conn,
        :DPI_ORACLE_TYPE_RAW,
        :DPI_NATIVE_TYPE_BYTES,
        byte_size(binary),
        byte_size(binary),
        true,
        false,
        :null
      ])

      oranif(slave, :stmt_bindByName, [statement, ":#{index + 1}", v])
      oranif(slave, :var_setFromBytes, [v, 0, binary])

      v
    end
  end

  defp define_param(slave, conn, statement, index, %DateTime{} = time) do
    define_param(slave, conn, statement, index, DateTime.to_naive(time))
  end

  defp define_param(slave, conn, statement, index, %NaiveDateTime{
         year: year,
         month: month,
         day: day,
         hour: hour,
         minute: minute,
         second: second,
         microsecond: {ms, 6}
       }) do
    %{var: v, data: [data]} =
      oranif(slave, :conn_newVar, [
        conn,
        :DPI_ORACLE_TYPE_TIMESTAMP,
        :DPI_NATIVE_TYPE_TIMESTAMP,
        1,
        0,
        false,
        false,
        :null
      ])
    oranif(slave, :stmt_bindByName, [statement, ":#{index + 1}", v])

    :ok =
      oranif(slave, :data_setTimestamp, [
        data,
        year,
        month,
        day,
        hour,
        minute,
        second,
        ms * 1000,
        0,
        0
      ])

    v
  end

  defp define_param(slave, conn, statement, index, %NaiveDateTime{
         year: year,
         month: month,
         day: day,
         hour: hour,
         minute: minute,
         second: second,
         microsecond: {_, 0}
       }) do
    %{var: v, data: [data]} =
      oranif(slave, :conn_newVar, [
        conn,
        :DPI_ORACLE_TYPE_TIMESTAMP,
        :DPI_NATIVE_TYPE_TIMESTAMP,
        1,
        0,
        false,
        false,
        :null
      ])

    oranif(slave, :stmt_bindByName, [statement, ":#{index + 1}", v])

    :ok =
      oranif(slave, :data_setTimestamp, [data, year, month, day, hour, minute, second, 0, 0, 0])
    v
  end

  defp define_param(slave, conn, statement, index, integer) when is_integer(integer) do
    %{var: v, data: [data]} =
      oranif(slave, :conn_newVar, [
        conn,
        :DPI_ORACLE_TYPE_NATIVE_INT,
        :DPI_NATIVE_TYPE_INT64,
        1,
        0,
        false,
        false,
        :null
      ])

    oranif(slave, :stmt_bindByName, [statement, ":#{index + 1}", v])
    :ok = oranif(slave, :data_setInt64, [data, integer])
    v
  end

  defp define_param(slave, conn, statement, index, float) when is_float(float) do
    %{var: v, data: [data]} =
      oranif(slave, :conn_newVar, [
        conn,
        :DPI_ORACLE_TYPE_NATIVE_DOUBLE,
        :DPI_NATIVE_TYPE_DOUBLE,
        1,
        0,
        false,
        false,
        :null
      ])

    oranif(slave, :stmt_bindByName, [statement, ":#{index + 1}", v])
    :ok = oranif(slave, :data_setDouble, [data, float])
    v
  end

  @impl true
  def handle_declare(
        %Jamdb.Oracle.Query{statement: statement} = query,
        _params,
        _opts,
        %OraLixir.Connection{oranifNode: slave} = state
      ) do
    case oranif(slave, :stmt_execute, [statement, []]) do
      numberOfColumns when is_integer(numberOfColumns) ->
        query = %{query | numCols: numberOfColumns}
        {:ok, query, statement, state}

      {:error, error} ->
        {:error, oranif_error(error), state}
    end
  end

  @impl true
  def handle_fetch(
        %Jamdb.Oracle.Query{numCols: numberOfColumns},
        statement,
        _opts,
        %OraLixir.Connection{oranifNode: slave} = state
      ) do
    case oranif(slave, :stmt_fetch, [statement]) do
      %{found: true} ->
        {:cont, fetch_row(numberOfColumns, slave, statement, []), state}

      %{found: false} ->
        {:halt, :halt, state}

      {:error, error} ->
        {:error, oranif_error(error), state}
    end
  end

  @impl true
  def handle_status(_opts, %{mode: mode} = state) do
    {mode, state}
  end

  @impl true
  def ping(%OraLixir.Connection{conn: conn, oranifNode: slave, mode: :idle} = state) do
    case oranif(slave, :conn_ping, [conn]) do
      :ok -> {:ok, state}
      error -> {:disconnect, error, state}
    end
  end

  def ping(%{mode: :transaction} = s) do
    {:ok, s}
  end

  defp create_context_connection(ora, opts) do
    username = Keyword.get(opts, :username, "scott")
    password = Keyword.get(opts, :password, "tiger")

    connectString =
      case Keyword.fetch(opts, :connectString) do
        {:ok, connStr} ->
          connStr

        :error ->
          port = Keyword.get(opts, :port, 1521)
          host = Keyword.get(opts, :hostname, "127.0.0.1")
          service_name = Keyword.get(opts, :service_name, "XE")

          """
          (DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=#{host})
          (PORT=#{port})))(CONNECT_DATA=(SERVER=dedicated)
          (SERVICE_NAME=#{service_name})))
          """
      end

    commonParams = Keyword.get(opts, :commonParams, %{})
    createParams = Keyword.get(opts, :createParams, %{})

    oranif(
      ora.oranifNode,
      :context_create,
      [@dpiMajorVersion, @dpiMinorVersion]
    )
    |> case do
      {:error, reason} ->
        {:error, reason}

      context ->
        oranif(
          ora.oranifNode,
          :conn_create,
          [
            context,
            username,
            password,
            connectString,
            commonParams,
            createParams
          ]
        )
        |> case do
          {:error, reason} ->
            {:error, reason}

          conn ->
            %{ora | context: context, conn: conn}
        end
    end
    |> case do
      {:error, reason} ->
        if ora.conn != nil, do: oranif(ora.oranifNode, :conn_close, [ora.conn, [], ""])
        if ora.context != nil, do: oranif(ora.oranifNode, :context_destroy, [ora.context])
#        if ora.oranifNode != node(), do: :dpi.unload(ora.oranifNode)
        {:error, reason}

      newora ->
        {:ok, newora}
    end
  end

  defp fetch_all(slave, statement, numberOfColumns) do
    case oranif(slave, :stmt_fetch, [statement]) do
      %{found: false} ->
        []

      %{found: true} ->
        [
          fetch_row(numberOfColumns, slave, statement, [])
          | fetch_all(slave, statement, numberOfColumns)
        ]
    end
  end

  defp fetch_row(0, _slave, _statement, row), do: row

  defp fetch_row(colIdx, slave, statement, row) do
    %{data: data} = oranif(slave, :stmt_getQueryValue, [statement, colIdx])
    value = case oranif(slave, :data_get, [data]) do
      {:error, error} ->
        IO.inspect(error, label: :error)
        IO.inspect(colIdx, label: :colIdx)
        IO.inspect(row, label: :row)
        raise "error!"
      v -> v
    end
    oranif(slave, :data_release, [data])
    fetch_row(colIdx - 1, slave, statement, [value | row])
  end

  defp execute_query(
         numberOfColumns,
         %Jamdb.Oracle.Query{statement: statement, info: %{:isQuery => true}} = query,
         %OraLixir.Connection{oranifNode: slave} = state
       )
       when numberOfColumns > 0 do
    columns =
      for idx <- 1..numberOfColumns do
        case oranif(slave, :stmt_getQueryInfo, [statement, idx])do
          col when is_map(col) -> col
          error -> raise error
        end
      end

    rows = fetch_all(slave, statement, numberOfColumns)
    result = %OraLixir.Result{columns: columns, rows: rows, num_rows: length(rows)}
    {:ok, %{query | numCols: numberOfColumns}, result, state}
  end

  defp execute_query(
         0,
         %Jamdb.Oracle.Query{statement: statement, info: %{statementType: statement_type}} = query,
         %OraLixir.Connection{oranifNode: slave} = state
       ) when statement_type in [:DPI_STMT_TYPE_UPDATE, :DPI_STMT_TYPE_INSERT] do
    count = oranif(slave, :stmt_getRowCount, [statement])
    {:ok, %{query | numCols: 0}, %OraLixir.Result{rows: nil, num_rows: count}, state}
  end

  defp execute_query(
         0,
         %Jamdb.Oracle.Query{statement: statement, info: %{:isQuery => false}} = query,
         %OraLixir.Connection{oranifNode: slave} = state
       ) do
    {:ok, %{query | numCols: 0}, %OraLixir.Result{rows: nil, num_rows: 0}, state}
  end

  defp oranif_error(%{reason: %{message: message}} = error) do
    %OraLixir.Error{message: message, details: error}
  end

  defp oranif_error(%{reason: message} = error) when is_list(message)do
    %OraLixir.Error{message: message, details: error}
  end

  defp oranif_error(error) do
    %OraLixir.Error{message: error}
  end
end
