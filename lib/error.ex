defmodule Jamdb.Oracle.Error do
  defexception [:message, :oracle, :query, :params]

  def exception(opts) do
    message = Keyword.get(opts, :message)
    query = Keyword.get(opts, :query)
    params = Keyword.get(opts, :params)

    oracle = build_meta(message)
    message = build_message(query, message, params)

    %Jamdb.Oracle.Error{oracle: oracle, message: message, query: query, params: params}
  end

  defp build_message(query, message, params) do
    IO.iodata_to_binary([
      message,
      build_query_message(query),
      build_bind_vars_messsage(params)
    ])
  end

  defp build_query_message(nil), do: []
  defp build_query_message(query), do: ["\n\n", "Query: #{query}"]

  defp build_bind_vars_messsage([]), do: []
  defp build_bind_vars_messsage(params), do: ["\n\n", "Bind Variables: #{inspect(params)}"]

  defp build_meta(message) do
    code = %{oracle_code: parse_error_code(message)}
    Map.merge(code, build_details(code, message))
  end

  # ORA-02291: integrity constraint (SYSTEM.BANK_TRANSACTIONS_PATIENT_ID_FKEY) violated - parent key not found
  defp build_details(%{oracle_code: "ORA-02291"}, message),
    do: %{code: :foreign_key_violation, constraint: parse_constraint(message)}

  # ORA-02292: integrity constraint (SYSTEM.PRESCRIPTION_ORDER_ITEMS_INVENTORY_ITEM_ID_FKEY) violated - child record found
  defp build_details(%{oracle_code: "ORA-02292"}, message),
    do: %{code: :foreign_key_violation, constraint: parse_constraint(message)}

  # ORA-00001: unique constraint (SYSTEM.DOCTORS__LOWER_EMAIL_INDEX) violated
  defp build_details(%{oracle_code: "ORA-00001"}, message),
    do: %{code: :unique_violation, constraint: parse_constraint(message)}

  defp build_details(_code, _message), do: %{}

  @error_code ~r/(?<error>ORA-.*)\:.*/
  defp parse_error_code(message) do
    case Regex.run(@error_code, message) do
      [_, code] -> code
      _ -> :unknown
    end
  end

  @constraint ~r/.*\((.*\.)?(?<name>.*)\)/
  defp parse_constraint(message) do
    case Regex.run(@constraint, message) do
      [_, _, name] ->
        String.downcase(name)
      _ ->
        "unknown"
    end
  end
end
