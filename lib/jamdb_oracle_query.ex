defmodule Jamdb.Oracle.Query do
  @moduledoc """
  Adapter module for Oracle. `DBConnection.Query` protocol implementation.

  See `DBConnection.prepare_execute/4`.

  """

  defstruct [:statement, :name, :batch, :query_rows_count, :result_types]

  alias Ecto.Query.{BooleanExpr, JoinExpr, QueryExpr}

  @doc false
  def all(query) do
    sources = create_names(query)

    from     = from(query, sources)
    select   = select(query, sources)
    window   = window(query, sources)
    join     = join(query, sources)
    where    = where(query, sources)
    group_by = group_by(query, sources)
    having   = having(query, sources)
    combinations = combinations(query)
    order_by = order_by(query, sources)
    limit    = limit(query, sources)
    offset   = offset(query, sources)
    lock     = lock(query.lock)

    [select, window, from, join, where, group_by, having, combinations, order_by, offset, limit | lock]
  end

  @doc false
  def update_all(%{from: %{source: source}} = query, prefix \\ nil) do
    sources = create_names(query)
    {from, name} = get_source(query, sources, 0, source)

    prefix = prefix || ["UPDATE ", from, ?\s, name | " SET "]
    fields = update_fields(query, sources)
    where = where(%{query | wheres: query.wheres}, sources)

    [prefix, fields, where | returning(query, sources)]
  end

  @doc false
  def delete_all(%{from: from} = query) do
    sources = create_names(query)
    {from, name} = get_source(query, sources, 0, from)

    where = where(%{query | wheres: query.wheres}, sources)
    
    ["DELETE FROM ", from, ?\s, name, where | returning(query, sources)]
  end

  def insert(_prefix, _table, _header, rows, on_conflict, returning)
      when length(rows) > 1 and length(returning) > 0,
      do: raise "Batch insert doesn't support returning values!"

  def insert(_prefix, _table, _header, rows, on_conflict, {targets, _, _})
      when is_list(targets) and length(rows) > 1,
      do: raise "Upsert doesn't support batch insert!"

  def insert(prefix, table, header, rows, on_conflict, returning) do
    values =
      if header == [] do
        [" VALUES " | intersperse_map(rows, ?,, fn _ -> "(DEFAULT)" end)]
      else
        [?\s, ?(, intersperse_map(header, ?,, &quote_name/1), ") VALUES " | insert_all([header], 1)]
      end

    table_name = quote_table(prefix, table)

    case on_conflict do
      [] ->
        batch_insert(["INSERT INTO ", table_name, values], header, rows, returning(returning))
      {:raise, _, []} ->
        batch_insert(["INSERT INTO ", table_name, values], header, rows, returning(returning))
      {:nothing, _, []} ->
        [
          "begin", ?\s,
          ["INSERT INTO ", table_name, values], ?;, ?\s,
          "exception", ?\s,
          "when dup_val_on_index then", ?\s,
            "NULL;", ?\s,
          "end;"
        ]
      {:nothing, _, targets} ->
        batch_insert(["INSERT ", ignore_conflict(table_name, targets), " INTO ", table_name, values], header, rows, returning(returning))
      {targets, _, [conflict]} when is_list(targets) ->
        [
          "begin", ?\s,
            ["INSERT INTO ", table_name, values | returning(returning)], ?;, ?\s,
          "exception", ?\s,
            "when dup_val_on_index then", ?\s,
              "UPDATE ", table_name, ?\s,
              "SET ", intersperse_map(targets, ?,, fn t -> [quote_name(t), " = :", Integer.to_string(position(header, t))] end), ?\s,
              "WHERE ", quote_name(conflict), " = :", Integer.to_string(position(header, conflict)), ?;, ?\s,
          "end;"
        ]
      {query, _, targets} ->
        [
          "begin", ?\s,
          ["INSERT INTO ", table_name, values | returning(returning)], ?;, ?\s,
          "exception", ?\s,
          "when dup_val_on_index then", ?\s,
          "UPDATE ", table_name, ?\s,
          "SET ", update_all(query, ""), ?\s,
          "WHERE ", intersperse_map(targets, ?,, fn t -> [quote_name(t), " = :", Integer.to_string(position(header, t))] end), ?;, ?\s,
          "end;"
        ]
    end
  end


  defp batch_insert(query, header, rows, _returning) when length(rows) > 1 do
    {:batch, length(header), query}
  end
  defp batch_insert(query, _header, _rows, returning) do
    query ++ returning
  end

  defp position(columns, column) do
    Enum.find_index(columns, fn c -> c == column end) + 1
  end

  defp insert_all(rows, counter) do
    intersperse_reduce(rows, ?,, counter, fn row, counter ->
      {row, counter} = insert_each(row, counter)
      {[?(, row, ?)], counter}
    end)
    |> elem(0)
  end

  defp insert_each(values, counter) do
    intersperse_reduce(values, ?,, counter, fn
      nil, counter ->
        {"DEFAULT", counter}
      _, counter ->
        {[?: | Integer.to_string(counter)], counter + 1}
    end)
  end

  defp ignore_conflict(table, targets),
       do: ["/*+ IGNORE_ROW_ON_DUPKEY_INDEX(", table, ?,, ?\s, conflict_target(targets) |") */"]
  defp conflict_target([]),
       do: []
  defp conflict_target({:unsafe_fragment, fragment}),
       do: [fragment, ?\s]
  defp conflict_target(targets),
       do: [?(, intersperse_map(targets, ?,, &quote_name/1), ?), ?\s]

  @doc false
  def update(prefix, table, fields, filters, returning) do
    {fields, count} = intersperse_reduce(fields, ", ", 1, fn field, acc ->
      {[quote_name(field), " = :" | Integer.to_string(acc)], acc + 1}
    end)

    {filters, _count} = intersperse_reduce(filters, " AND ", count, fn
      {field, nil}, acc ->
        {[quote_name(field), " IS NULL"], acc}

      {field, _value}, acc ->
        {[quote_name(field), " = :" | Integer.to_string(acc)], acc + 1}
    end)

    ["UPDATE ", quote_table(prefix, table), " SET ",
     fields, " WHERE ", filters | returning(returning)]
  end

  @doc false
  def delete(prefix, table, filters, returning) do
    {filters, _} = intersperse_reduce(filters, " AND ", 1, fn
      {field, nil}, acc ->
        {[quote_name(field), " IS NULL"], acc}

      {field, _value}, acc ->
        {[quote_name(field), " = :" | Integer.to_string(acc)], acc + 1}
    end)

    ["DELETE FROM ", quote_table(prefix, table), " WHERE ", filters | returning(returning)]
  end

  @doc false
  def table_exists_query(table) do
    {"SELECT count(*) FROM user_tables WHERE table_name = :1 ", [table]}
  end

  @doc false
  def ddl_logs(_result), do: []

  @doc false
  def to_constraints(_err, _opts \\ []), do: []

  ## Query generation

  binary_ops =
    [==: " = ", !=: " != ", <=: " <= ", >=: " >= ", <: " < ", >: " > ",
     +: " + ", -: " - ", *: " * ", /: " / ",
     and: " AND ", or: " OR ", like: " LIKE "]

  @binary_ops Keyword.keys(binary_ops)

  Enum.map(binary_ops, fn {op, str} ->
    defp handle_call(unquote(op), 2), do: {:binary_op, unquote(str)}
  end)

  defp handle_call(fun, _arity), do: {:fun, Atom.to_string(fun)}

  defp select(%{select: %{fields: fields}, distinct: distinct} = query, sources) do
    ["SELECT ", distinct(distinct, sources, query) | select_fields(fields, sources, query)]
  end

  @reserved_keywords [:comment, :size, :number, :current]

  defp select_fields([], _sources, _query),
    do: "NULL"
  defp select_fields(fields, sources, query) do
    intersperse_map(fields, ", ", fn
      # @reserved_keywords are reserved in Oracle, so we need to quote it
      # NB: "column_name" and column_name are not equal from Oracle perspective!
      {key, value} when key in @reserved_keywords ->
        [expr(value, sources, query), ?\s, ?"] ++ quote_name(key) ++ [?"]
      {key, value} ->
        [expr(value, sources, query), ?\s | quote_name(key)]
      value ->
        expr(value, sources, query)
    end)
  end

  defp distinct(nil, _, _), do: []
  defp distinct(%QueryExpr{expr: []}, _, _), do: {[], []}
  defp distinct(%QueryExpr{expr: true}, _, _), do: "DISTINCT "
  defp distinct(%QueryExpr{expr: false}, _, _), do: []
  defp distinct(%QueryExpr{expr: exprs}, _, _) when is_list(exprs), do: "DISTINCT "

  defp from(%{from: %{hints: [_ | _]}} = query, _sources) do
    error!(query, "table hints are not supported")
  end

  defp from(%{from: %{source: source}} = query, sources) do
    {from, name} = get_source(query, sources, 0, source)
    [" FROM ", from, ?\s | name]
  end

  defp update_fields(%{updates: updates} = query, sources) do
    for(%{expr: expr} <- updates,
        {op, kw} <- expr,
        {key, value} <- kw,
        do: update_op(op, key, value, sources, query)) |> Enum.intersperse(", ")
  end

  defp update_op(:set, key, value, sources, query) do
    [quote_name(key), " = " | expr(value, sources, query)]
  end

  defp update_op(:inc, key, value, sources, query) do
    [quote_name(key), " = ", quote_qualified_name(key, sources, 0), " + " |
     expr(value, sources, query)]
  end

  defp update_op(command, _key, _value, _sources, query) do
    error!(query, "unknown update operation #{inspect command}")
  end

  defp join(%{joins: []}, _sources), do: []
  defp join(%{joins: joins} = query, sources) do
    [?\s | intersperse_map(joins, ?\s, fn
      %JoinExpr{on: %QueryExpr{expr: expr}, qual: qual, ix: ix, source: source, hints: hints} ->
        if hints != [] do
          error!(query, "table hints are not supported")
        end

        {join, name} = get_source(query, sources, ix, source)
        [join_qual(qual), join, " ", name | join_on(qual, expr, sources, query)]
    end)]
  end

  defp join_on(:cross, true, _sources, _query), do: []
  defp join_on(:left_lateral, true, _sources, _query), do: []
  defp join_on(:inner_lateral, true, _sources, _query), do: []
  defp join_on(_qual, expr, sources, query), do: [" ON " | expr(expr, sources, query)]

  defp join_qual(:inner), do: "INNER JOIN "
  defp join_qual(:left),  do: "LEFT OUTER JOIN "
  defp join_qual(:left_lateral),  do: "OUTER APPLY "
  defp join_qual(:inner_lateral),  do: "CROSS APPLY "
  defp join_qual(:right), do: "RIGHT OUTER JOIN "
  defp join_qual(:full),  do: "FULL OUTER JOIN "
  defp join_qual(:cross), do: "CROSS JOIN "

  defp where(%{wheres: wheres} = query, sources) do
    boolean(" WHERE ", wheres, sources, query)
  end

  defp having(%{havings: havings} = query, sources) do
    boolean(" HAVING ", havings, sources, query)
  end

  defp group_by(%{group_bys: []}, _sources), do: []
  defp group_by(%{group_bys: group_bys} = query, sources) do
    [" GROUP BY " |
     intersperse_map(group_bys, ", ", fn
       %QueryExpr{expr: expr} ->
         intersperse_map(expr, ", ", &expr(&1, sources, query))
     end)]
  end

  defp window(%{windows: []}, _sources), do: []
  defp window(%{windows: windows} = query, sources) do
    intersperse_map(windows, ", ", fn
      {_, %{expr: kw}} ->
        window_exprs(kw, sources, query)
    end)
  end

  defp window_exprs(kw, sources, query) do
    [?(, intersperse_map(kw, ?\s, &window_expr(&1, sources, query)), ?)]
  end

  defp window_expr({:partition_by, fields}, sources, query) do
    ["PARTITION BY " | intersperse_map(fields, ", ", &expr(&1, sources, query))]
  end

  defp window_expr({:order_by, fields}, sources, query) do
    ["ORDER BY " | intersperse_map(fields, ", ", &order_by_expr(&1, sources, query))]
  end

  defp window_expr({:frame, {:fragment, _, _} = fragment}, sources, query) do
    expr(fragment, sources, query)
  end

  defp order_by(%{order_bys: []}, _sources), do: []
  defp order_by(%{order_bys: order_bys} = query, sources) do
    [" ORDER BY " |
     intersperse_map(order_bys, ", ", fn
       %QueryExpr{expr: expr} ->
         intersperse_map(expr, ", ", &order_by_expr(&1, sources, query))
     end)]
  end

  defp order_by_expr({dir, expr}, sources, query) do
    str = order_by_expression(expr, sources, query)

    case dir do
      :asc  -> str
      :asc_nulls_last -> [str | " ASC NULLS LAST"]
      :asc_nulls_first -> [str | " ASC NULLS FIRST"]
      :desc -> [str | " DESC"]
      :desc_nulls_last -> [str | " DESC NULLS LAST"]
      :desc_nulls_first -> [str | " DESC NULLS FIRST"]
    end
  end

  defp order_by_expression({:==, _, _} = expr, sources, query) do
    [?(, "CASE WHEN ", expr(expr, sources, query), " THEN 1 ELSE 0 END", ?)]
  end

  defp order_by_expression(expr, sources, query) do
    expr(expr, sources, query)
  end

  defp limit(%{limit: nil}, _sources), do: []
  defp limit(%{limit: %QueryExpr{expr: expr}} = query, sources) do
    [" FETCH NEXT ", expr(expr, sources, query), " ROWS ONLY"]
  end
  
  defp offset(%{offset: nil}, _sources), do: []
  defp offset(%{offset: %QueryExpr{expr: expr}} = query, sources) do
    [" OFFSET ", expr(expr, sources, query), " ROWS"]
  end

  defp combinations(%{combinations: combinations}) do
    Enum.map(combinations, fn
      {:union, query} -> [" UNION (", all(query), ")"]
      {:union_all, query} -> [" UNION ALL (", all(query), ")"]
      {:except, query} -> [" MINUS (", all(query), ")"]
      {:intersect, query} -> [" INTERSECT (", all(query), ")"]
    end)
  end

  defp lock(nil), do: []
  defp lock(lock_clause), do: [?\s | lock_clause]

  defp boolean(_name, [], _sources, _query), do: []
  defp boolean(name, [%{expr: expr, op: op} | query_exprs], sources, query) do
    [name |
     Enum.reduce(query_exprs, {op, paren_expr(expr, sources, query)}, fn
       %BooleanExpr{expr: expr, op: op}, {op, acc} ->
         {op, [acc, operator_to_boolean(op), paren_expr(expr, sources, query)]}
       %BooleanExpr{expr: expr, op: op}, {_, acc} ->
         {op, [?(, acc, ?), operator_to_boolean(op), paren_expr(expr, sources, query)]}
     end) |> elem(1)]
  end

  defp operator_to_boolean(:and), do: " AND "
  defp operator_to_boolean(:or), do: " OR "

  defp parens_for_select([first_expr | _] = expr) do
    if is_binary(first_expr) and String.starts_with?(first_expr, ["SELECT", "select"]) do
      [?(, expr, ?)]
    else
      expr
    end
  end

  defp paren_expr(expr, sources, query) do
    [expr(expr, sources, query)]
  end

  defp expr({:^, [], [ix]}, _sources, _query) do
    [?: | Integer.to_string(ix + 1)]
  end

  defp expr({{:., _, [{:&, _, [idx]}, field]}, _, []}, sources, _query) when is_atom(field) do
    quote_qualified_name(field, sources, idx)
  end

  defp expr({:&, _, [idx]}, sources, _query) do
    {_, source, _} = elem(sources, idx)
    source
  end

  defp expr({:in, _, [_left, []]}, _sources, _query) do
    "false"
  end

  defp expr({:in, _, [left, right]}, sources, query) when is_list(right) do
    args =
      intersperse_map(right, ?,, fn
        elem when is_list(elem) -> [?(, intersperse_map(elem, ?,, &expr(&1, sources, query)), ?)]
        elem -> expr(elem, sources, query)
      end)
    [expr(left, sources, query), " IN (", args, ?)]
  end

  # Building condition for `where(Team, [t], t.id in ^[])`
  # in SQL it will be `WHERE (1 = 2)
  defp expr({:in, _, [_left, {:^, _, [_, 0]}]}, _sources, _query) do
    "1 = 2"
  end

  # Building condition for `where(Team, [t], t.id in ^[1,2])`
  # in SQL it will be `WHERE (t.id IN (:1, :2)) | [1,2]`
  defp expr({:in, _, [left, {:^, _, [ix, length]}]}, sources, query) do
    right = for s <- 1..length, do: [?:, Integer.to_string(ix + s)]
    [expr(left, sources, query), " IN (", Enum.intersperse(right, ?,), ?)]
  end

  defp expr({:in, _, [left, right]}, sources, query) do
    [expr(left, sources, query), " = ANY(", expr(right, sources, query), ?)]
  end

  defp expr({:is_nil, _, [arg]}, sources, query) do
    [expr(arg, sources, query) | " IS NULL"]
  end

  defp expr({:not, _, [expr]}, sources, query) do
    ["NOT (", expr(expr, sources, query), ?)]
  end

  defp expr(%Ecto.SubQuery{query: query}, _sources, _query) do
    [?(, all(query), ?)]
  end

  defp expr({:fragment, _, [kw]}, _sources, query) when is_list(kw) or tuple_size(kw) == 3 do
    error!(query, "keyword or interpolated fragments are not supported")
  end

  defp expr({:fragment, _, parts}, sources, query) do
    Enum.map(parts, fn
      {:raw, part}  -> part
      {:expr, expr} -> expr(expr, sources, query)
    end)
    |> parens_for_select
  end

  defp expr({:date_add, _, [date, count, interval]}, sources, query) do
    interval(date, " + ", count, interval, sources, query)
  end

  defp expr({:datetime_add, _, [datetime, count, interval]}, sources, query) do
    interval(datetime, " + ", count, interval, sources, query)
  end

  defp expr({:from_now, _, [count, interval]}, sources, query) do
    interval(DateTime.utc_now, " + ", count, interval, sources, query)
  end

  defp expr({:ago, _, [count, interval]}, sources, query) do
    interval(DateTime.utc_now, " - ", count, interval, sources, query)
  end

  defp expr({:over, _, [agg, name]}, sources, query) when is_atom(name) do
    aggregate = expr(agg, sources, query)
    [aggregate, " OVER "]
  end

  defp expr({:over, _, [agg, kw]}, sources, query) do
    aggregate = expr(agg, sources, query)
    [aggregate, " OVER ", window_exprs(kw, sources, query)]
  end

  defp expr({:{}, _, elems}, sources, query) do
    [?(, intersperse_map(elems, ?,, &expr(&1, sources, query)), ?)]
  end

  defp expr({:count, _, []}, _sources, _query), do: "count(*)"

  defp expr({:ilike, _, [col, bind]}, sources, query) do
    [?(, "LOWER", ?(, expr(col, sources, query), ?), " LIKE LOWER (", expr(bind, sources, query), ?), ?)]
  end

  defp expr({fun, _, args}, sources, query) when is_atom(fun) and is_list(args) do
    {modifier, args} =
      case args do
        [rest, :distinct] -> {"DISTINCT ", [rest]}
        _ -> {[], args}
      end

    case handle_call(fun, length(args)) do
      {:binary_op, op} ->
        [left, right] = args
        [op_to_binary(left, sources, query), op | op_to_binary(right, sources, query)]
      {:fun, fun} ->
        [fun, ?(, modifier, intersperse_map(args, ", ", &expr(&1, sources, query)), ?)]
    end
  end

  defp expr(%Decimal{} = decimal, _sources, _query) do
    Decimal.to_string(decimal, :normal)
  end

  defp expr(%Ecto.Query.Tagged{value: literal}, sources, query) do
    expr(literal, sources, query)
  end

  defp expr(nil, _sources, _query),   do: "NULL"
  defp expr(true, _sources, _query),  do: "1"
  defp expr(false, _sources, _query), do: "0"

  defp expr(literal, _sources, _query) when is_binary(literal) or is_list(literal) do
     ["'", escape_string(literal), "'"]
  end

  defp expr(literal, _sources, _query) when is_integer(literal) do
    Integer.to_string(literal)
  end

  defp expr(literal, _sources, _query) when is_float(literal) do
    Float.to_string(literal)
  end

  defp interval(datetime, literal, count, interval, sources, query) do
    [?(, expr(datetime, sources, query), literal, " INTERVAL '", 
         expr(count, sources, query), "' ", interval, ?)]
  end

  defp op_to_binary({op, _, [_, _]} = expr, sources, query) when op in @binary_ops do
    paren_expr(expr, sources, query)
  end

  defp op_to_binary(expr, sources, query) do
    expr(expr, sources, query)
  end

  defp returning(%{select: nil}, _sources),
    do: []
  defp returning(%{select: %{fields: fields}} = query, sources) do
    [{:&, _, [_idx, returning, _counter]}] = fields
    [" RETURN ", select_fields(fields, sources, query),
     " INTO ", intersperse_map(returning, ", ", &[?: | quote_name(&1)])]
  end
  
  defp returning([]),
    do: []
  defp returning(fields) do
    returning = fields |> Enum.filter(& is_tuple(&1) == false)
    [" RETURN ", intersperse_map(returning, ", ", &quote_name/1),
     " INTO ", intersperse_map(returning, ", ", &[?: | quote_name(&1)])]
  end   

  defp create_names(%{sources: sources}) do
    create_names(sources, 0, tuple_size(sources)) |> List.to_tuple()
  end

  defp create_names(sources, pos, limit) when pos < limit do
    [create_name(sources, pos) | create_names(sources, pos + 1, limit)]
  end

  defp create_names(_sources, pos, pos) do
    []
  end

  defp create_name(sources, pos) do
    case elem(sources, pos) do
      {:fragment, _, _} ->
        {nil, [?f | Integer.to_string(pos)], nil}

      {table, schema, prefix} ->
        name = [create_alias(table) | Integer.to_string(pos)]
        {quote_table(prefix, table), name, schema}

      %Ecto.SubQuery{} ->
        {nil, [?s | Integer.to_string(pos)], nil}
    end
  end

  defp create_alias(<<first, _rest::binary>>) when first in ?a..?z when first in ?A..?Z do
    <<first>>
  end
  defp create_alias(_) do
    "t"
  end

  # DDL

  alias Ecto.Migration.{Table, Index, Reference, Constraint}

  def execute_ddl({command, %Table{} = table, columns}) when command in [:create, :create_if_not_exists] do
    table_name = quote_table(table.prefix, table.name)
    query = [if_do(command == :create_if_not_exists, :begin),
             "CREATE TABLE ",
             table_name, ?\s, ?(,
             column_definitions(table, columns), pk_definition(columns, ", "), ?),
             options_expr(table.options),
             if_do(command == :create_if_not_exists, :end)]
    
    [query] ++
      comments_on("TABLE", table_name, table.comment) ++
      comments_for_columns(table_name, columns)
  end

  def execute_ddl({command, %Table{} = table}) when command in [:drop, :drop_if_exists] do
    [[if_do(command == :drop_if_exists, :begin),
      "DROP TABLE ", quote_table(table.prefix, table.name),
      if_do(command == :drop_if_exists, :end)]]
  end

  def execute_ddl({:alter, %Table{} = table, changes}) do
    table_name = quote_table(table.prefix, table.name)
    query = ["ALTER TABLE ", table_name, ?\s,
             column_changes(table, changes), pk_definition(changes, ", ADD ")]

    [query] ++
      comments_on("TABLE", table_name, table.comment) ++
      comments_for_columns(table_name, changes)
  end

  def execute_ddl({command, %Index{} = index}) when command in [:create, :create_if_not_exists] do
    [[if_do(command == :create_if_not_exists, :begin),
      "CREATE", if_do(index.unique, " UNIQUE"), " INDEX ",
      quote_name(index.name),
      " ON ",
      quote_table(index.prefix, index.table), ?\s,
      index_columns(index),
      if_do(command == :create_if_not_exists, :end)]]
  end

  def execute_ddl({command, %Index{} = index}) when command in [:drop, :drop_if_exists] do
    [[if_do(command == :drop_if_exists, :begin),
      "DROP INDEX ", quote_table(index.prefix, index.name),
      if_do(command == :drop_if_exists, :end)]]
  end

  def execute_ddl({:rename, %Table{} = current_table, %Table{} = new_table}) do
    [["RENAME ", quote_table(current_table.prefix, current_table.name),
      " TO ", quote_table(nil, new_table.name)]]
  end

  def execute_ddl({:rename, %Table{} = table, current_column, new_column}) do
    [["ALTER TABLE ", quote_table(table.prefix, table.name), " RENAME COLUMN ",
      quote_name(current_column), " TO ", quote_name(new_column)]]
  end

  def execute_ddl({command, %Constraint{} = constraint}) when command in [:create, :create_if_not_exists] do
    [[if_do(command == :create_if_not_exists, :begin),
      "ALTER TABLE ", quote_table(constraint.prefix, constraint.table),
      " ADD CONSTRAINT ", quote_name(constraint.name), constraint_expr(constraint),
      if_do(command == :create_if_not_exists, :end)]]
  end

  def execute_ddl({command, %Constraint{} = constraint}) when command in [:drop, :drop_if_exists] do
    [[if_do(command == :drop_if_exists, :begin),
      "ALTER TABLE ", quote_table(constraint.prefix, constraint.table),
      " DROP CONSTRAINT ", quote_name(constraint.name),
      if_do(command == :drop_if_exists, :end)]]
  end

  def execute_ddl(string) when is_binary(string), do: [string]

  def execute_ddl(keyword) when is_list(keyword),
    do: error!(nil, "keyword lists in execute are not supported")

  defp pk_definition(columns, prefix) do
    pks =
      for {_, name, _, opts} <- columns,
          opts[:primary_key],
          do: name

    case pks do
      [] -> []
      _  -> [prefix, "PRIMARY KEY (", intersperse_map(pks, ", ", &quote_name/1), ")"]
    end
  end

  defp comments_on(_object, _name, nil), do: []
  defp comments_on(object, name, comment) do
    [["COMMENT ON ", object, ?\s, name, " IS ", single_quote(comment)]]
  end

  defp comments_for_columns(table_name, columns) do
    Enum.flat_map(columns, fn
      {_operation, column_name, _column_type, opts} ->
        column_name = [table_name, ?. | quote_name(column_name)]
        comments_on("COLUMN", column_name, opts[:comment])
      _ -> []
    end)
  end

  defp column_definitions(table, columns) do
    intersperse_map(columns, ", ", &column_definition(table, &1))
  end

  defp column_definition(table, {:add, name, %Reference{} = ref, opts}) do
    [quote_name(name), ?\s, column_type(ref.type, opts),
     column_options(ref.type, opts), reference_expr(ref, table, name)]
  end

  defp column_definition(_table, {:add, name, type, opts}) do
    [quote_name(name), ?\s, column_type(type, opts), column_options(type, opts)]
  end

  defp column_changes(table, columns) do
    cond do
      same_changes(:add, columns) ->
        ["ADD(" | intersperse_map(columns, ", ", &column_change(table, &1))] ++ [")"]
      same_changes(:remove, columns) ->
        ["DROP(" | intersperse_map(columns, ", ", &column_change(table, &1))] ++ [")"]
      true ->
        intersperse_map(columns, ", ", &column_change(table, &1))
    end
  end

  defp same_changes(change, columns) do
    columns |> Enum.all?(fn (column) -> Kernel.elem(column, 0) == change end)
  end

  defp column_change(table, {:add, name, %Reference{} = ref, opts}) do
    [quote_name(name), ?\s, column_type(ref.type, opts),
     column_options(ref.type, opts), reference_expr(ref, table, name)]
  end

  defp column_change(_table, {:add, name, type, opts}) do
    [quote_name(name), ?\s, column_type(type, opts), column_options(type, opts)]
  end

  defp column_change(table, {:modify, name, %Reference{} = ref, opts}) do
    [drop_constraint_expr(opts[:from], table, name), "MODIFY ", quote_name(name), " ", column_type(ref.type, opts),
     constraint_expr(ref, table, name), modify_null(name, opts), modify_default(name, ref.type, opts)]
  end

  defp column_change(table, {:modify, name, type, opts}) do
    [drop_constraint_expr(opts[:from], table, name), "MODIFY ", quote_name(name), " ",
     column_type(type, opts), modify_null(name, opts), modify_default(name, type, opts)]
  end

  defp column_change(_table, {:remove, name}), do: [quote_name(name)]
  defp column_change(table, {:remove, name, %Reference{} = ref, _opts}) do
    [drop_constraint_expr(ref, table, name), "DROP COLUMN ", quote_name(name)]
  end
  defp column_change(_table, {:remove, name, _type, _opts}), do: [quote_name(name)]

  defp modify_null(_name, opts) do
    case Keyword.get(opts, :null) do
      true  -> [" NULL"]
      false -> [" NOT NULL"]
      nil   -> []
      # true  -> [", ALTER COLUMN ", quote_name(name), " DROP NOT NULL"]
      # false -> [", ALTER COLUMN ", quote_name(name), " SET NOT NULL"]
      # nil   -> []
    end
  end

  defp modify_default(_name, _type, opts) do
    case Keyword.fetch(opts, :default) do
      {:ok, val} -> [default_expr({:ok, val})]
      # {:ok, val} -> [", ALTER COLUMN ", quote_name(name), " SET", default_expr({:ok, val})]
      :error -> []
    end
  end

  defp column_options(type, opts) do
    default     = Keyword.fetch(opts, :default)
    primary_key = Keyword.get(opts, :primary_key)
    null        = Keyword.get(opts, :null)

    [default_expr(type, default, primary_key), null_expr(null)]
  end

  defp null_expr(false), do: " NOT NULL"
  defp null_expr(true), do: " NULL"
  defp null_expr(_), do: []

  defp default_expr(:bigserial, :error, true),
    do: " GENERATED BY DEFAULT ON NULL AS IDENTITY"
  defp default_expr(_type, default, _primary_key),
    do: default_expr(default)
  defp default_expr({:ok, nil}),
    do: " DEFAULT NULL"
  # defp default_expr({:ok, []}),
    # do: " DEFAULT '[]'"
  defp default_expr({:ok, literal}) when is_binary(literal),
    do: [" DEFAULT '", escape_string(literal), ?']
  defp default_expr({:ok, true}),
    do: [" DEFAULT 1"]
  defp default_expr({:ok, false}),
    do: [" DEFAULT 0"]
  defp default_expr({:ok, literal}) when is_number(literal) or is_boolean(literal),
    do: [" DEFAULT ", to_string(literal)]
  defp default_expr({:ok, {:fragment, expr}}),
    do: [" DEFAULT ", expr]
  defp default_expr({:ok, value}) when is_map(value) or is_list(value),
    do: " DEFAULT '#{Jamdb.Oracle.json_library().encode!(value)}'"
  defp default_expr(:error),
    do: []

  defp index_columns(%Index{where: nil, columns: columns}), do: [?(, intersperse_map(columns, ", ", &index_expr/1), ?)]

  # CREATE UNIQUE INDEX billing_items_name_index ON billing_items (
  #  case when provider_id IS NULL then id || ‘/‘ name end
  # )
  defp index_columns(%Index{where: where, columns: columns}) do
    [?(,
      "CASE WHEN ",
      where,
      " THEN ",
      columns |> Enum.map(&index_expr/1) |> Enum.join(" || '/' || "),
      " END",
      ?)
    ]
  end

  defp index_expr(literal) when is_binary(literal),
    do: literal
  defp index_expr(literal),
    do: quote_name(literal)

  defp options_expr(nil),
    do: []
  defp options_expr(keyword) when is_list(keyword),
    do: error!(nil, "keyword lists in :options are not supported")
  defp options_expr(options),
    do: [?\s, options]

  defp column_type({:array, type}, opts),
    do: [column_type(type, opts)]

  defp column_type(type, _opts) when type in ~w(utc_datetime naive_datetime)a,
    do: [ecto_to_db(type), "(0)"]

  defp column_type(type, opts) when type in ~w(utc_datetime_usec naive_datetime_usec)a do
    precision = Keyword.get(opts, :precision)
    type_name = ecto_to_db(type)

    if precision do
      [type_name, ?(, to_string(precision), ?)]
    else
      type_name
    end
  end

  defp column_type(type, opts) do
    size      = Keyword.get(opts, :size)
    precision = Keyword.get(opts, :precision)
    scale     = Keyword.get(opts, :scale)
    national  = Keyword.get(opts, :national, false)
    type_name = [if_do(national and type in [:string, :binary], "n"), ecto_to_db(type)]

    cond do
      size            -> [type_name, ?(, to_string(size), ?)]
      precision       -> [type_name, ?(, to_string(precision), ?,, to_string(scale || 0), ?)]
      type == :string -> [type_name, "(255)"]
      true            -> type_name
    end
  end

  defp reference_expr(%Reference{} = ref, table, name),
    do: [" CONSTRAINT ", reference_name(ref, table, name), " REFERENCES ",
         quote_table(ref.prefix || table.prefix, ref.table), ?(, quote_name(ref.column), ?),
         reference_on_delete(ref.on_delete)]

  defp constraint_expr(%Reference{} = ref, table, name),
    do: [", ADD CONSTRAINT ", reference_name(ref, table, name), ?\s,
         "FOREIGN KEY (", quote_name(name), ") REFERENCES ",
         quote_table(ref.prefix || table.prefix, ref.table), ?(, quote_name(ref.column), ?),
         reference_on_delete(ref.on_delete)]

  defp drop_constraint_expr(%Reference{} = ref, table, name),
    do: ["DROP CONSTRAINT ", reference_name(ref, table, name), ", "]
  defp drop_constraint_expr(_, _, _),
    do: []

  defp reference_name(%Reference{name: nil}, table, column),
    do: quote_name("#{table.name}_#{column}_fkey")
  defp reference_name(%Reference{name: name}, _table, _column),
    do: quote_name(name)

  defp constraint_expr(%Constraint{check: check}) when is_binary(check), 
    do: [" CHECK ", ?(, check, ?)]
  defp constraint_expr(_),
    do: []

  defp reference_on_delete(:nilify_all), do: " ON DELETE SET NULL"
  defp reference_on_delete(:delete_all), do: " ON DELETE CASCADE"
  defp reference_on_delete(_), do: []

  defp ecto_to_db(:id),                  do: "integer"
  defp ecto_to_db(:binary_id),           do: "raw(16)"
  defp ecto_to_db(:uuid),                do: "raw(16)"
  defp ecto_to_db(:bigint),              do: "integer"
  defp ecto_to_db(:bigserial),           do: "integer"
  defp ecto_to_db(:integer),             do: "integer"
  defp ecto_to_db(:float),               do: "number"
  defp ecto_to_db(:boolean),             do: "integer" #char(1)"
  defp ecto_to_db(:string),              do: "varchar2"
  defp ecto_to_db(:binary),              do: "clob"
  defp ecto_to_db(:text),                do: "clob"
  defp ecto_to_db({:array, _}),          do: "clob"
  defp ecto_to_db(:jsonb),               do: "clob" # use Application.fetch_env!(:ecto_sql, :postgres_map_type)
  defp ecto_to_db(:map),                 do: "clob" # use Application.fetch_env!(:ecto_sql, :postgres_map_type)
  defp ecto_to_db({:map, _}),            do: "clob" # use Application.fetch_env!(:ecto_sql, :postgres_map_type)
  defp ecto_to_db(:decimal),             do: "decimal"
  defp ecto_to_db(:naive_datetime),      do: "timestamp"
  defp ecto_to_db(:time),                do: "timestamp"
  defp ecto_to_db(:naive_datetime_usec), do: "timestamp"
  defp ecto_to_db(:utc_datetime),        do: "timestamp"
  defp ecto_to_db(:utc_datetime_usec),   do: "timestamp"
  defp ecto_to_db(:inet),                do: "varchar2(255)"
  defp ecto_to_db(other),                do: Atom.to_string(other)

  defp single_quote(value), do: [?', escape_string(value), ?']

  defp if_do(condition, :begin) do
    if condition, do: "BEGIN EXECUTE IMMEDIATE '", else: []
  end
  defp if_do(condition, :end) do
    if condition, do: "'; EXCEPTION WHEN OTHERS THEN NULL; END;", else: []
  end
  defp if_do(condition, value) do
    if condition, do: value, else: []
  end

  ## Helpers

  defp get_source(query, sources, ix, source) do
    {expr, name, _schema} = elem(sources, ix)
    {expr || paren_expr(source, sources, query), name}
  end

  defp quote_qualified_name(name, sources, ix) do
    {_, source, _} = elem(sources, ix)
    [source, ?. | quote_name(name)]
  end

  defp quote_name(name) when is_atom(name) do
    quote_name(Atom.to_string(name))
  end
  defp quote_name(name) do
    if String.contains?(name, "\"") do
      error!(nil, "bad field name #{inspect name}")
    end
    [name] # identifiers are not case sensitive
  end

  defp quote_table(nil, name),    do: quote_table(name)
  defp quote_table(prefix, name), do: [quote_table(prefix), ?., quote_table(name)]

  defp quote_table(name) when is_atom(name),
    do: quote_table(Atom.to_string(name))
  defp quote_table(name) do
    if String.contains?(name, "\"") do
      error!(nil, "bad table name #{inspect name}")
    end
     [name] # identifiers are not case sensitive
  end

  defp intersperse_map(list, separator, mapper, acc \\ [])
  defp intersperse_map([], _separator, _mapper, acc),
    do: acc
  defp intersperse_map([elem], _separator, mapper, acc),
    do: [acc | mapper.(elem)]
  defp intersperse_map([elem | rest], separator, mapper, acc),
    do: intersperse_map(rest, separator, mapper, [acc, mapper.(elem), separator])

  defp intersperse_reduce(list, separator, user_acc, reducer, acc \\ [])
  defp intersperse_reduce([], _separator, user_acc, _reducer, acc),
    do: {acc, user_acc}
  defp intersperse_reduce([elem], _separator, user_acc, reducer, acc) do
    {elem, user_acc} = reducer.(elem, user_acc)
    {[acc | elem], user_acc}
  end
  defp intersperse_reduce([elem | rest], separator, user_acc, reducer, acc) do
    {elem, user_acc} = reducer.(elem, user_acc)
    intersperse_reduce(rest, separator, user_acc, reducer, [acc, elem, separator])
  end

  defp escape_string(value) when is_list(value) do
    escape_string(:binary.list_to_bin(value))
  end
  defp escape_string(value) when is_binary(value) do
    :binary.replace(value, "'", "''", [:global])
  end

  defp error!(nil, msg) do
    raise ArgumentError, msg
  end
  defp error!(query, msg) do
    raise Ecto.QueryError, query: query, message: msg
  end
  
end

defimpl String.Chars, for: Jamdb.Oracle.Query do
  def to_string(%Jamdb.Oracle.Query{statement: statement}) do
    IO.iodata_to_binary(statement)
  end
end
