defmodule Jamdb.Oracle.Extensions.Numeric do
  def decode([rows]), do: [Enum.map(rows, &decode_number/1)]
  def decode(rows), do: rows

  defp decode_number({value}), do: decode_number(value)
  defp decode_number(value) when is_number(value), do: Decimal.cast(value)
  defp decode_number(value), do: value
end
