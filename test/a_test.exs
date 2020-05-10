defmodule ATest do
  use ExUnit.Case
  alias Jamdb.Oracle.TestRepo

  defmodule A do
    use Ecto.Schema
     @primary_key false
#    @primary_key {:id, :binary_id, autogenerate: true}
#    @foreign_key_type :binary_id
#    @timestamps_opts [type: :utc_datetime_usec]

    schema "foo" do
      field :value, :boolean
    end
  end

  test "foo" do
    IO.inspect TestRepo.insert(%A{value: false}, on_conflict: :nothing)
    IO.inspect TestRepo.all(A)
  end
end
