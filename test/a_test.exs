defmodule ATest do
  use ExUnit.Case
  alias Jamdb.Oracle.TestRepo

  defmodule A do
    use Ecto.Schema
     @primary_key false
#    @primary_key {:id, :binary_id, autogenerate: true}
#    @foreign_key_type :binary_id
#    @timestamps_opts [type: :utc_datetime_usec]

    schema "schema_migrations" do
      field :version, :integer
    end
  end

  test "foo" do
    IO.inspect TestRepo.all(A)
  end
end
