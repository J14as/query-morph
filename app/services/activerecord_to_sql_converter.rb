# frozen_string_literal: true

# Converts ActiveRecord method chains into equivalent SQL statements.
#
# Supports: where, where.not, order, limit, offset, select, joins,
#           left_outer_joins, group, having, distinct, count, sum,
#           average, minimum, maximum, find, find_by, first, last,
#           create/create!, update_all, destroy_all/delete_all
class ActiverecordToSqlConverter
  class ConversionError < StandardError; end

  def initialize(ar_code)
    @ar_code = ar_code.strip
  end

  def convert
    model_name, chain = split_model_and_chain(@ar_code)
    table             = model_to_table(model_name)
    calls             = tokenize(chain)
    ctx               = fresh_ctx(table)

    calls.each { |method, args| apply(method, args, ctx) }

    build_sql(ctx)
  rescue ConversionError
    raise
  rescue => e
    raise ConversionError, "Parse error: #{e.message}"
  end

  private

  # ─── Initial context ──────────────────────────────────────────────────────

  def fresh_ctx(table)
    {
      table:    table,
      select:   nil,          # nil means SELECT table.*
      distinct: false,
      joins:    [],
      where:    [],           # array of SQL strings
      group:    nil,
      having:   nil,
      order:    nil,
      limit:    nil,
      offset:   nil,
      # Write ops
      write_op:  nil,         # :insert, :update, :delete
      write_data: nil
    }
  end

  # ─── Split "User.where(...)" into model="User" and chain=".where(...)" ─────

  def split_model_and_chain(code)
    unless code =~ /\A([A-Z][A-Za-z0-9:]*)(\..*)?/m
      raise ConversionError,
            "Expected chain to start with a capitalized model name (e.g., User.where(...))"
    end
    [$1, $2.to_s]
  end

  # ─── Tokenize the method chain ────────────────────────────────────────────
  # Returns array of [method_name, args_string] pairs.
  # Handles nested parentheses correctly.

  def tokenize(chain)
    calls = []
    str   = chain.dup

    until str.empty?
      # Consume leading dot
      str.sub!(/\A\./, '')
      break if str.empty?

      # Method name
      unless str =~ /\A(\w+(?:\.\w+)*)(\(|\.|$)/
        str = str.sub(/\A[^.(]+/, '')
        next
      end
      method = $1
      rest   = str[method.length..]

      if rest.start_with?('(')
        args, consumed = extract_parens(rest)
        calls << [method, args]
        str = rest[consumed..]
      else
        # Method with no parentheses (e.g., .all, .distinct, .first)
        calls << [method, '']
        str = rest
      end
    end

    calls
  end

  # Extracts content inside balanced parentheses. Returns [content, chars_consumed].
  def extract_parens(str)
    raise ConversionError, "Expected '('" unless str.start_with?('(')

    depth   = 0
    in_str  = false
    str_ch  = nil
    content = ''

    str.chars.each_with_index do |ch, i|
      if in_str
        content += ch if i > 0
        in_str = false if ch == str_ch
      elsif ch == '"' || ch == "'"
        in_str = true; str_ch = ch
        content += ch if i > 0
      elsif ch == '('
        depth += 1
        content += ch if i > 0
      elsif ch == ')'
        depth -= 1
        if depth == 0
          return [content, i + 1]
        end
        content += ch
      else
        content += ch if i > 0
      end
    end

    raise ConversionError, "Unbalanced parentheses in: #{str[0..40]}"
  end

  # ─── Apply each method call to the context ────────────────────────────────

  def apply(method, args, ctx)
    case method

    # ── Read ────────────────────────────────────────────────────────────────

    when 'all'
      # no-op; SELECT table.*

    when 'find'
      ctx[:where] << "\"#{ctx[:table]}\".\"id\" = #{args.strip}"

    when 'find_by'
      parse_hash_conditions(args).each { |k, v| ctx[:where] << "\"#{ctx[:table]}\".\"#{k}\" = #{sql_val(v)}" }

    when 'where'
      parse_where(args, ctx).each { |c| ctx[:where] << c }

    when 'not'
      # Handles where.not – we'll catch this via compound method 'where.not' in chain
      # This is a no-op if called standalone

    when 'or'
      # Approximate or: wrap existing where in OR with new condition
      new_conditions = parse_where(args, ctx)
      unless new_conditions.empty?
        existing = ctx[:where].join(' AND ')
        new_cond = new_conditions.join(' AND ')
        ctx[:where] = ["(#{existing}) OR (#{new_cond})"]
      end

    when 'select'
      cols = parse_columns(args, ctx[:table])
      ctx[:select] = cols

    when 'pluck'
      cols = parse_columns(args, ctx[:table])
      ctx[:select] = cols

    when 'distinct'
      ctx[:distinct] = true

    when 'joins'
      assoc  = unquote_sym(args)
      joined = assoc_to_table(assoc)
      ctx[:joins] << "INNER JOIN \"#{joined}\" ON \"#{joined}\".\"#{ctx[:table].chomp('s')}_id\" = \"#{ctx[:table]}\".\"id\""

    when 'left_outer_joins', 'left_joins'
      assoc  = unquote_sym(args)
      joined = assoc_to_table(assoc)
      ctx[:joins] << "LEFT OUTER JOIN \"#{joined}\" ON \"#{joined}\".\"#{ctx[:table].chomp('s')}_id\" = \"#{ctx[:table]}\".\"id\""

    when 'includes', 'eager_load', 'preload'
      # These affect loading strategy; SQL effect ~ joins
      assoc  = unquote_sym(args)
      joined = assoc_to_table(assoc)
      ctx[:joins] << "INNER JOIN \"#{joined}\" ON \"#{joined}\".\"#{ctx[:table].chomp('s')}_id\" = \"#{ctx[:table]}\".\"id\""

    when 'order'
      ctx[:order] = parse_order(args, ctx[:table])

    when 'reorder'
      ctx[:order] = parse_order(args, ctx[:table])

    when 'group'
      ctx[:group] = parse_columns(args, ctx[:table])

    when 'having'
      ctx[:having] = parse_raw_condition(args)

    when 'limit'
      ctx[:limit] = args.strip.to_i

    when 'offset'
      ctx[:offset] = args.strip.to_i

    when 'first'
      ctx[:limit]  = args.strip.empty? ? 1 : args.strip.to_i
      ctx[:order] ||= "\"#{ctx[:table]}\".\"id\" ASC"

    when 'last'
      ctx[:limit]  = args.strip.empty? ? 1 : args.strip.to_i
      ctx[:order]  = "\"#{ctx[:table]}\".\"id\" DESC"

    when 'count'
      col = args.strip.empty? ? '*' : quote_col(unquote_sym(args), ctx[:table])
      ctx[:select] = "COUNT(#{col})"

    when 'sum'
      ctx[:select] = "SUM(#{quote_col(unquote_sym(args), ctx[:table])})"

    when 'average'
      ctx[:select] = "AVG(#{quote_col(unquote_sym(args), ctx[:table])})"

    when 'minimum'
      ctx[:select] = "MIN(#{quote_col(unquote_sym(args), ctx[:table])})"

    when 'maximum'
      ctx[:select] = "MAX(#{quote_col(unquote_sym(args), ctx[:table])})"

    # ── Write ────────────────────────────────────────────────────────────────

    when 'create', 'create!', 'new', 'insert'
      ctx[:write_op]   = :insert
      ctx[:write_data] = parse_hash_conditions(args)

    when 'update_all'
      ctx[:write_op]   = :update
      ctx[:write_data] = parse_hash_conditions(args)

    when 'update'
      # .update(attrs) on a found record – treat like update_all for SQL output
      unless args =~ /\A\d+\z/
        ctx[:write_op]   = :update
        ctx[:write_data] = parse_hash_conditions(args)
      end

    when 'destroy_all', 'delete_all', 'destroy', 'delete'
      ctx[:write_op] = :delete
    end
  end

  # ─── WHERE parsing ────────────────────────────────────────────────────────

  def parse_where(args, ctx)
    args = args.strip
    return [] if args.empty?

    # where.not style: caller passes prefixed "NOT:…" — handled separately

    # String condition: "col = ?", val1, val2 OR "raw SQL"
    if args =~ /\A"([^"]+)"\s*(?:,\s*(.+))?\z/m
      template = $1
      params   = $2 ? split_args($2) : []
      conditions = []
      template.split(/\s+AND\s+/i).each do |part|
        filled = part.dup
        filled.gsub!('?') { ruby_to_sql_val(params.shift&.strip || '?') }
        conditions << "(#{filled})"
      end
      return conditions
    end

    # where.not hash style
    if args.start_with?('NOT:')
      hash = parse_hash_conditions(args[4..])
      return hash.map { |k, v|
        if v == 'nil' || v == 'NULL'
          "\"#{ctx[:table]}\".\"#{k}\" IS NOT NULL"
        else
          "\"#{ctx[:table]}\".\"#{k}\" != #{sql_val_raw(v)}"
        end
      }
    end

    # Hash conditions: col: val, col: val
    hash = parse_hash_conditions(args)
    unless hash.empty?
      return hash.map { |k, v|
        if v == 'nil' || v == 'NULL' || v.nil?
          "\"#{ctx[:table]}\".\"#{k}\" IS NULL"
        elsif v =~ /\A\[(.+)\]\z/
          vals = $1.split(',').map { |x| ruby_to_sql_val(x.strip) }.join(', ')
          "\"#{ctx[:table]}\".\"#{k}\" IN (#{vals})"
        else
          "\"#{ctx[:table]}\".\"#{k}\" = #{sql_val_raw(v)}"
        end
      }
    end

    # Fallthrough: raw condition string
    ["(#{args.gsub(/\A['"]|['"]\z/, '')})"]
  end

  # Parse hash-style args: "name: 'Alice', age: 30" → {name: "'Alice'", age: "30"}
  def parse_hash_conditions(args)
    result = {}
    args   = args.strip

    # Scan key: value pairs
    # Handles: key: value, key: "str", key: 'str', key: :sym, key: [1,2]
    scanner = args.dup
    until scanner.empty?
      scanner.lstrip!
      break unless scanner =~ /\A(\w+):\s*/
      key     = $1
      scanner = scanner[$&.length..]

      # Read value (respects brackets and quotes)
      val, consumed = read_value(scanner)
      result[key] = val
      scanner = scanner[consumed..].lstrip
      scanner = scanner[1..].lstrip if scanner.start_with?(',')
    end

    result
  end

  # Reads one Ruby value from the front of the string.
  # Returns [value_string, chars_consumed]
  def read_value(str)
    str = str.dup
    # Array [...]
    if str.start_with?('[')
      depth = 0
      str.chars.each_with_index do |ch, i|
        depth += 1 if ch == '['
        depth -= 1 if ch == ']'
        return [str[0..i], i + 1] if depth == 0
      end
    end
    # Quoted string
    if str.start_with?('"') || str.start_with?("'")
      q = str[0]
      i = 1
      while i < str.length
        return [str[0..i], i + 1] if str[i] == q && str[i-1] != '\\'
        i += 1
      end
    end
    # Symbol :foo
    if str.start_with?(':')
      m = str.match(/\A:\w+/)
      return [m[0], m[0].length] if m
    end
    # Number, boolean, nil
    m = str.match(/\A(?:nil|true|false|\d+(?:\.\d+)?)/)
    return [m[0], m[0].length] if m

    # Read until comma or closing paren
    m = str.match(/\A[^,)\]]+/)
    return [m ? m[0].rstrip : str, m ? m[0].length : str.length]
  end

  def parse_raw_condition(args)
    args = args.strip
    # quoted string → raw SQL
    args.gsub(/\A["']|["']\z/, '')
  end

  # ─── ORDER parsing ────────────────────────────────────────────────────────
  # Handles: :col, "col ASC", "col DESC", col: :asc, col: :desc

  def parse_order(args, table)
    args = args.strip

    # "col ASC" / "col DESC" as string
    if args =~ /\A["'](.+)["']\z/
      return $1  # already proper SQL
    end

    parts = split_args(args)
    parts.map { |part|
      part = part.strip
      if part =~ /\A(\w+):\s*:?(asc|desc)\z/i
        "\"#{table}\".\"#{$1}\" #{$2.upcase}"
      elsif part =~ /\A:(\w+)\z/
        "\"#{table}\".\"#{$1}\" ASC"
      else
        part
      end
    }.join(', ')
  end

  # ─── SELECT / GROUP column parsing ────────────────────────────────────────

  def parse_columns(args, table)
    parts = split_args(args)
    parts.map { |p|
      p = p.strip
      if p =~ /\A:(\w+)\z/
        "\"#{table}\".\"#{$1}\""
      elsif p =~ /\A"(.+)"\z/
        $1
      else
        p
      end
    }.join(', ')
  end

  # ─── SQL builder ──────────────────────────────────────────────────────────

  def build_sql(ctx)
    case ctx[:write_op]
    when :insert then build_insert_sql(ctx)
    when :update then build_update_sql(ctx)
    when :delete then build_delete_sql(ctx)
    else              build_select_sql(ctx)
    end
  end

  def build_select_sql(ctx)
    sel = ctx[:select] || "\"#{ctx[:table]}\".*"
    sel = "DISTINCT #{sel}" if ctx[:distinct] && !sel.start_with?('DISTINCT')

    sql  = "SELECT #{sel}"
    sql += " FROM \"#{ctx[:table]}\""
    sql += " #{ctx[:joins].join(' ')}"      unless ctx[:joins].empty?
    sql += " WHERE #{ctx[:where].join(' AND ')}" unless ctx[:where].empty?
    sql += " GROUP BY #{ctx[:group]}"       if ctx[:group]
    sql += " HAVING #{ctx[:having]}"        if ctx[:having]
    sql += " ORDER BY #{ctx[:order]}"       if ctx[:order]
    sql += " LIMIT #{ctx[:limit]}"          if ctx[:limit]
    sql += " OFFSET #{ctx[:offset]}"        if ctx[:offset]
    sql + ";"
  end

  def build_insert_sql(ctx)
    return "INSERT INTO \"#{ctx[:table]}\" -- (no attributes parsed);" if ctx[:write_data].nil? || ctx[:write_data].empty?

    cols = ctx[:write_data].keys.map { |k| "\"#{k}\"" }.join(', ')
    vals = ctx[:write_data].values.map { |v| sql_val_raw(v) }.join(', ')
    "INSERT INTO \"#{ctx[:table]}\" (#{cols}) VALUES (#{vals});"
  end

  def build_update_sql(ctx)
    return "UPDATE \"#{ctx[:table]}\" SET -- (no attributes parsed);" if ctx[:write_data].nil? || ctx[:write_data].empty?

    set_clause  = ctx[:write_data].map { |k, v| "\"#{k}\" = #{sql_val_raw(v)}" }.join(', ')
    sql = "UPDATE \"#{ctx[:table]}\" SET #{set_clause}"
    sql += " WHERE #{ctx[:where].join(' AND ')}" unless ctx[:where].empty?
    sql + ";"
  end

  def build_delete_sql(ctx)
    sql = "DELETE FROM \"#{ctx[:table]}\""
    sql += " WHERE #{ctx[:where].join(' AND ')}" unless ctx[:where].empty?
    sql + ";"
  end

  # ─── Value helpers ────────────────────────────────────────────────────────

  # Ruby value → SQL value
  def sql_val_raw(v)
    v = v.to_s.strip
    return 'NULL'      if v == 'nil' || v.empty?
    return 'TRUE'      if v == 'true'
    return 'FALSE'     if v == 'false'
    return v           if v =~ /\A\d+(?:\.\d+)?\z/
    return $1          if v =~ /\A["'](.*)["']\z/   # strip quotes, re-add as SQL
    return "'#{$1}'"   if v =~ /\A["'](.*)["']\z/
    # Symbol :foo → 'foo' (treat as string value)
    return "'#{$1}'"   if v =~ /\A:(\w+)\z/
    "'#{v}'"
  end

  def sql_val(ruby_val)
    sql_val_raw(ruby_val.to_s)
  end

  def ruby_to_sql_val(v)
    v = v.to_s.strip
    return 'NULL'  if v == 'nil'
    return 'TRUE'  if v == 'true'
    return 'FALSE' if v == 'false'
    return v       if v =~ /\A\d+(?:\.\d+)?\z/
    return "'#{$1}'" if v =~ /\A["'](.*)["']\z/
    "'#{v}'"
  end

  def quote_col(col, table)
    col =~ /\./ ? col : "\"#{table}\".\"#{col}\""
  end

  # ─── Model / Table name helpers ───────────────────────────────────────────

  # "User"      → "users"
  # "BlogPost"  → "blog_posts"
  def model_to_table(model)
    # Split on CamelCase boundaries
    snake = model.gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2')
                 .gsub(/([a-z\d])([A-Z])/, '\1_\2')
                 .downcase
    # Pluralize (simple rules)
    if snake.end_with?('y') && !%w[ay ey iy oy uy].any? { |v| snake.end_with?(v) }
      snake.sub(/y\z/, 'ies')
    elsif snake.end_with?('s', 'x', 'z', 'ch', 'sh')
      snake + 'es'
    else
      snake + 's'
    end
  end

  # :post → "posts"
  def assoc_to_table(assoc)
    model_to_table(assoc.split('_').map(&:capitalize).join)
  end

  def unquote_sym(args)
    args.strip.gsub(/\A[:"']|["']\z/, '')
  end

  def split_args(str)
    args = []
    current = ''
    depth = 0
    in_str = false
    str_ch = nil

    str.chars.each do |ch|
      if in_str
        current += ch
        in_str = false if ch == str_ch
      elsif ch == '"' || ch == "'"
        in_str = true; str_ch = ch; current += ch
      elsif ch =~ /[(\[{]/
        depth += 1; current += ch
      elsif ch =~ /[)\]}]/
        depth -= 1; current += ch
      elsif ch == ',' && depth == 0
        args << current.strip; current = ''
      else
        current += ch
      end
    end

    args << current.strip unless current.strip.empty?
    args
  end
end
