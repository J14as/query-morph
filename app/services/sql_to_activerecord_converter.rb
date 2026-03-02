# frozen_string_literal: true

# Converts SQL statements into equivalent ActiveRecord method chains.
#
# Supported: SELECT (with WHERE, ORDER BY, LIMIT, OFFSET, GROUP BY, HAVING,
#            INNER/LEFT JOIN, DISTINCT, aggregate functions),
#            INSERT, UPDATE, DELETE
class SqlToActiverecordConverter
  class ConversionError < StandardError; end

  def initialize(sql)
    @sql = sql.strip.chomp(';')
  end

  def convert
    norm = normalize(@sql)
    type = norm.split.first.upcase

    case type
    when 'SELECT' then convert_select(norm)
    when 'INSERT' then convert_insert(norm)
    when 'UPDATE' then convert_update(norm)
    when 'DELETE' then convert_delete(norm)
    else
      raise ConversionError,
            "Unsupported statement '#{type}'. Supported: SELECT, INSERT, UPDATE, DELETE."
    end
  rescue ConversionError
    raise
  rescue => e
    raise ConversionError, "Parse error: #{e.message}"
  end

  private

  # ─── Normalise whitespace ─────────────────────────────────────────────────

  def normalize(sql)
    sql.gsub(/\s+/, ' ').strip
  end

  # ─── SELECT ───────────────────────────────────────────────────────────────

  def convert_select(sql)
    ctx = {}

    # ── Extract clauses in reverse order of appearance ──────────────────────

    # LIMIT … OFFSET …
    if sql =~ /\bLIMIT\s+(\d+)\s+OFFSET\s+(\d+)/i
      ctx[:limit]  = $1.to_i
      ctx[:offset] = $2.to_i
      sql = sql.sub(/\s*LIMIT\s+\d+\s+OFFSET\s+\d+/i, '')
    elsif sql =~ /\bLIMIT\s+(\d+)/i
      ctx[:limit] = $1.to_i
      sql = sql.sub(/\s*LIMIT\s+\d+/i, '')
    end

    if sql =~ /\bOFFSET\s+(\d+)/i
      ctx[:offset] = $1.to_i
      sql = sql.sub(/\s*OFFSET\s+\d+/i, '')
    end

    # ORDER BY col [ASC|DESC] [, …]
    if sql =~ /\bORDER\s+BY\s+(.+?)(?=\s+(?:LIMIT|OFFSET|HAVING|GROUP\s+BY|$))/i
      ctx[:order] = $1.strip
      sql = sql.sub(/\s*ORDER\s+BY\s+.+?(?=\s+(?:LIMIT|OFFSET|HAVING|GROUP\s+BY|$))/i, '')
    end

    # HAVING …
    if sql =~ /\bHAVING\s+(.+?)(?=\s+(?:ORDER\s+BY|LIMIT|OFFSET|$))/i
      ctx[:having] = $1.strip
      sql = sql.sub(/\s*HAVING\s+.+?(?=\s+(?:ORDER\s+BY|LIMIT|OFFSET|$))/i, '')
    end

    # GROUP BY col [, …]
    if sql =~ /\bGROUP\s+BY\s+(.+?)(?=\s+(?:HAVING|ORDER\s+BY|LIMIT|OFFSET|$))/i
      ctx[:group] = $1.strip
      sql = sql.sub(/\s*GROUP\s+BY\s+.+?(?=\s+(?:HAVING|ORDER\s+BY|LIMIT|OFFSET|$))/i, '')
    end

    # WHERE …  (everything before any remaining JOIN / FROM but after WHERE)
    if sql =~ /\bWHERE\s+(.+?)(?=\s+(?:GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|OFFSET|$))/i
      ctx[:where] = $1.strip
      sql = sql.sub(/\s*WHERE\s+.+?(?=\s+(?:GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|OFFSET|$))/i, '')
    end

    # JOINs (may be multiple)
    joins = []
    join_re = /\b((?:INNER|LEFT\s+OUTER|LEFT|RIGHT\s+OUTER|RIGHT|FULL\s+OUTER|FULL|CROSS)?\s*JOIN)\s+(\w+)\s+ON\s+([^\b].*?)(?=\s+(?:(?:INNER|LEFT|RIGHT|FULL|CROSS)?\s*JOIN|WHERE|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|OFFSET|$))/i
    while sql =~ join_re
      joins << { type: $1.strip.upcase, table: $2.strip, condition: $3.strip }
      sql = sql.sub(join_re, '')
    end
    ctx[:joins] = joins

    # FROM table [alias]
    model = 'Model'
    if sql =~ /\bFROM\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?/i
      model = table_to_model($1)
    end

    # SELECT [DISTINCT] columns
    distinct = false
    cols     = '*'
    if sql =~ /\bSELECT\s+(DISTINCT\s+)?(.+?)\s+FROM\b/i
      distinct = !$1.nil?
      cols     = $2.strip
    end

    build_select_ar(model, cols, distinct, ctx)
  end

  def build_select_ar(model, cols, distinct, ctx)
    chain = model

    # ── JOINs ──────────────────────────────────────────────────────────────
    ctx[:joins].each do |j|
      assoc = table_to_assoc(j[:table])
      cond  = j[:condition]
      if j[:type].start_with?('LEFT')
        chain += ".left_outer_joins(:#{assoc})"
      elsif simple_fk_join?(cond, j[:table])
        chain += ".joins(:#{assoc})"
      else
        chain += ".joins(\"#{j[:type]} JOIN #{j[:table]} ON #{cond}\")"
      end
    end

    # ── WHERE ──────────────────────────────────────────────────────────────
    if ctx[:where]
      chain += build_where(ctx[:where])
    end

    # ── SELECT columns ─────────────────────────────────────────────────────
    # Handle aggregate-only selects that become terminal calls
    if cols =~ /\ACOUNT\(\*\)\z/i
      chain += ".count"
      return finalize(chain)
    elsif cols =~ /\ACOUNT\((\w+)\)\z/i
      chain += ".count(:#{$1})"
      return finalize(chain)
    elsif cols =~ /\ASUM\((\w+)\)\z/i
      chain += ".sum(:#{$1})"
      return finalize(chain)
    elsif cols =~ /\AAVG\((\w+)\)\z/i
      chain += ".average(:#{$1})"
      return finalize(chain)
    elsif cols =~ /\AMIN\((\w+)\)\z/i
      chain += ".minimum(:#{$1})"
      return finalize(chain)
    elsif cols =~ /\AMAX\((\w+)\)\z/i
      chain += ".maximum(:#{$1})"
      return finalize(chain)
    elsif cols != '*'
      chain += build_select_cols(cols)
    end

    chain += ".distinct" if distinct

    # ── GROUP BY ───────────────────────────────────────────────────────────
    if ctx[:group]
      g = ctx[:group].split(',').map { |c| sym_or_str(c.strip) }.join(', ')
      chain += ".group(#{g})"
    end

    # ── HAVING ─────────────────────────────────────────────────────────────
    chain += ".having(#{condition_string(ctx[:having])})" if ctx[:having]

    # ── ORDER BY ───────────────────────────────────────────────────────────
    chain += build_order(ctx[:order]) if ctx[:order]

    # ── LIMIT / OFFSET ─────────────────────────────────────────────────────
    chain += ".limit(#{ctx[:limit]})"   if ctx[:limit]
    chain += ".offset(#{ctx[:offset]})" if ctx[:offset]

    # If the chain is still just the bare model with no conditions → .all
    chain += ".all" if chain == model

    finalize(chain)
  end

  # ── WHERE builder ──────────────────────────────────────────────────────────

  def build_where(where_str)
    # Multiple AND conditions → try hash form where all are equality
    if where_str =~ /\bOR\b/i
      # OR conditions → always string form
      return ".where(#{condition_string(where_str)})"
    end

    parts = split_and_conditions(where_str)

    if parts.length == 1
      build_single_where(parts.first)
    else
      # Try to group simple equality conditions into one hash; rest as strings
      hash_pairs  = []
      str_clauses = []

      parts.each do |p|
        eq = try_hash_condition(p)
        if eq
          hash_pairs << eq
        else
          str_clauses << p.strip
        end
      end

      result = ''

      unless hash_pairs.empty?
        result += ".where(#{hash_pairs.join(', ')})"
      end

      str_clauses.each do |sc|
        result += build_single_where(sc)
      end

      result
    end
  end

  def build_single_where(cond)
    cond = cond.strip

    # id = N  → .find(N) shorthand (only for bare "id = N" without other stuff)
    if cond =~ /\Aid\s*=\s*(\d+)\z/i
      return ".find(#{$1})"  # Note: caller should NOT chain further – we'll leave it as is
    end

    eq = try_hash_condition(cond)
    return ".where(#{eq})" if eq

    # Fall back to string condition
    ".where(#{condition_string(cond)})"
  end

  # Returns "col: val" string if condition is simple equality, else nil
  def try_hash_condition(cond)
    cond = cond.strip

    # col = 'string'
    if cond =~ /\A(\w+)\s*=\s*'([^']*)'\z/
      return "#{$1}: \"#{$2}\""
    end

    # col = N  (integer / float)
    if cond =~ /\A(\w+)\s*=\s*(\d+(?:\.\d+)?)\z/
      return "#{$1}: #{$2}"
    end

    # col = true/false
    if cond =~ /\A(\w+)\s*=\s*(true|false)\z/i
      return "#{$1}: #{$2.downcase}"
    end

    # col IS NULL
    if cond =~ /\A(\w+)\s+IS\s+NULL\z/i
      return "#{$1}: nil"
    end

    nil
  end

  def condition_string(cond)
    # IS NOT NULL → string form
    if cond =~ /\A(\w+)\s+IS\s+NOT\s+NULL\z/i
      col = $1
      return "\"#{col} IS NOT NULL\""
    end

    # col IN (1, 2, 3) → col: [1, 2, 3]
    if cond =~ /\A(\w+)\s+IN\s*\(([^)]+)\)\z/i
      col  = $1
      vals = $2.split(',').map { |v| ruby_literal(v.strip) }.join(', ')
      return "#{col}: [#{vals}]"
    end

    # col NOT IN (…) → string
    if cond =~ /\A(\w+)\s+NOT\s+IN\s*\(([^)]+)\)\z/i
      col  = $1
      vals = $2.split(',').map(&:strip).join(', ')
      return "\"#{col} NOT IN (#{vals})\""
    end

    # col BETWEEN a AND b → string with ?-bindings
    if cond =~ /\A(\w+)\s+BETWEEN\s+(.+?)\s+AND\s+(.+)\z/i
      col = $1; a = ruby_literal($2.strip); b = ruby_literal($3.strip)
      return "\"#{col} BETWEEN ? AND ?\", #{a}, #{b}"
    end

    # col LIKE 'pattern' → string
    if cond =~ /\A(\w+(?:\.\w+)?)\s+LIKE\s+'([^']*)'\z/i
      col = $1.split('.').last
      return "\"#{col} LIKE ?\", \"%#{$2.gsub('%','').gsub('_','').strip}%\""
    end

    # col != val or col <> val
    if cond =~ /\A(\w+(?:\.\w+)?)\s*(?:!=|<>)\s*(.+)\z/
      col = $1.split('.').last
      val = ruby_literal($2.strip)
      return "\"#{col} != ?\", #{val}"
    end

    # col >= / <= / > / < val  → string with ?-binding
    if cond =~ /\A(\w+(?:\.\w+)?)\s*(>=|<=|>|<)\s*(.+)\z/
      col = $1.split('.').last
      op  = $2
      val = ruby_literal($3.strip)
      return "\"#{col} #{op} ?\", #{val}"
    end

    # Default: wrap in double-quotes as raw string
    "\"#{cond.gsub('"', '\\"')}\""
  end

  # ── ORDER builder ──────────────────────────────────────────────────────────

  def build_order(order_str)
    parts = order_str.split(',').map do |part|
      part = part.strip
      if part =~ /\A(\w+(?:\.\w+)?)\s+(ASC|DESC)\z/i
        col = $1.split('.').last
        dir = $2.downcase.to_sym
        dir == :asc ? ":#{col}" : "#{col}: :desc"
      else
        col = part.split('.').last
        ":#{col}"   # ASC default
      end
    end

    ".order(#{parts.join(', ')})"
  end

  # ── SELECT column builder ──────────────────────────────────────────────────

  def build_select_cols(cols)
    # If any col has a space (alias), keep as string
    col_list = cols.split(',').map { |c| c.strip }

    all_simple = col_list.all? { |c| c =~ /\A\w+(?:\.\w+)?\z/ }

    if all_simple
      syms = col_list.map { |c| ":#{c.split('.').last}" }.join(', ')
      ".select(#{syms})"
    else
      ".select(\"#{cols}\")"
    end
  end

  # ── INSERT ────────────────────────────────────────────────────────────────

  def convert_insert(sql)
    # Match: INSERT INTO table (cols) VALUES (vals)
    # We extract cols and vals using bracket-aware extraction, NOT [^)]+ regex,
    # so that values like NOW(), NEXTVAL(...) work correctly.
    unless sql =~ /INSERT\s+INTO\s+(\w+)\s*\(/i
      raise ConversionError,
            "Cannot parse INSERT. Expected: INSERT INTO table (col1, col2) VALUES (val1, val2)"
    end

    table_name = $1
    rest       = sql[$&.length - 1..]  # from the opening '(' of columns

    cols_content, after_cols = extract_balanced(rest)
    raise ConversionError, "Cannot find column list in INSERT." unless cols_content

    unless after_cols.upcase =~ /\s*VALUES\s*\(/
      raise ConversionError, "Cannot find VALUES clause in INSERT."
    end

    vals_start = after_cols.index('(', after_cols.upcase.index('VALUES'))
    raise ConversionError, "Cannot find VALUES list in INSERT." unless vals_start

    vals_content, = extract_balanced(after_cols[vals_start..])
    raise ConversionError, "Cannot extract VALUES list." unless vals_content

    model = table_to_model(table_name)
    cols  = split_values(cols_content).map(&:strip)
    vals  = split_values_balanced(vals_content)

    pairs = cols.zip(vals).map { |c, v| "#{c}: #{ruby_literal(v.strip)}" }.join(', ')
    "#{model}.create(#{pairs})"
  end

  # Extract content inside balanced parentheses at the start of str.
  # Returns [inner_content, rest_of_string] or nil if no match.
  def extract_balanced(str)
    return nil unless str.start_with?('(')
    depth = 0
    content = ''
    in_str = false
    str_ch = nil
    str.chars.each_with_index do |ch, i|
      if in_str
        content += ch if i > 0
        in_str = false if ch == str_ch
      elsif ch == "'" || ch == '"'
        in_str = true; str_ch = ch
        content += ch if i > 0
      elsif ch == '('
        depth += 1
        content += ch if i > 0
      elsif ch == ')'
        depth -= 1
        if depth == 0
          return [content, str[i + 1..]]
        end
        content += ch
      else
        content += ch if i > 0
      end
    end
    nil
  end

  # Split a VALUES list that may contain function calls like NOW(), NEXTVAL('seq')
  def split_values_balanced(str)
    values = []
    current = ''
    depth = 0
    in_str = false
    str_ch = nil
    str.chars.each do |ch|
      if in_str
        current += ch
        in_str = false if ch == str_ch
      elsif ch == "'" || ch == '"'
        in_str = true; str_ch = ch; current += ch
      elsif ch == '('
        depth += 1; current += ch
      elsif ch == ')'
        depth -= 1; current += ch
      elsif ch == ',' && depth == 0
        values << current.strip; current = ''
      else
        current += ch
      end
    end
    values << current.strip unless current.strip.empty?
    values
  end

  # ── UPDATE ────────────────────────────────────────────────────────────────

  def convert_update(sql)
    unless sql =~ /UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?\z/i
      raise ConversionError, "Cannot parse UPDATE statement."
    end

    model     = table_to_model($1)
    set_str   = $2.strip
    where_str = $3&.strip

    set_pairs = set_str.split(',').map { |pair|
      k, v = pair.split('=', 2).map(&:strip)
      "#{k}: #{ruby_literal(v)}"
    }.join(', ')

    if where_str
      "#{model}#{build_where(where_str)}.update_all(#{set_pairs})"
    else
      "#{model}.update_all(#{set_pairs})"
    end
  end

  # ── DELETE ────────────────────────────────────────────────────────────────

  def convert_delete(sql)
    unless sql =~ /DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?\z/i
      raise ConversionError, "Cannot parse DELETE statement."
    end

    model     = table_to_model($1)
    where_str = $2&.strip

    if where_str
      "#{model}#{build_where(where_str)}.destroy_all"
    else
      "#{model}.destroy_all"
    end
  end

  # ─── Helpers ──────────────────────────────────────────────────────────────

  # "users" → "User", "blog_posts" → "BlogPost"
  def table_to_model(table)
    singular = table.gsub(/s\z/, '').gsub(/ies\z/, 'y')
    singular.split('_').map(&:capitalize).join
  end

  # "posts" → "post" (for use as :association_name)
  def table_to_assoc(table)
    name = table.downcase
    name.end_with?('ies') ? name.sub(/ies\z/, 'y').sub(/\z/, '') :
      name.end_with?('s')  ? name.sub(/s\z/, '')   : name
  end

  # Does the JOIN condition look like a standard FK join we can represent as :assoc?
  def simple_fk_join?(condition, _table)
    condition =~ /\w+\.\w+_id\s*=\s*\w+\.id/i
  end

  def split_and_conditions(where_str)
    # Split on AND, respecting parentheses and quoted strings
    parts   = []
    current = ''
    depth   = 0
    in_str  = false
    str_ch  = nil
    i       = 0

    while i < where_str.length
      ch = where_str[i]

      if in_str
        current += ch
        in_str = false if ch == str_ch
        i += 1; next
      end

      if ch == "'" || ch == '"'
        in_str = true; str_ch = ch; current += ch
        i += 1; next
      end

      if ch == '('
        depth += 1; current += ch; i += 1; next
      end

      if ch == ')'
        depth -= 1; current += ch; i += 1; next
      end

      # Check for ' AND ' (case-insensitive) only at depth 0
      if depth == 0 && where_str[i..i+4].upcase == ' AND '
        parts << current.strip unless current.strip.empty?
        current = ''
        i += 5  # skip ' AND ' (5 chars)
        next
      end

      current += ch
      i += 1
    end

    parts << current.strip unless current.strip.empty?
    parts
  end

  # Split a VALUES list respecting quoted strings
  def split_values(str)
    values = []
    current = ''
    in_quote = false
    str.chars.each do |ch|
      if ch == "'" && !in_quote
        in_quote = true; current += ch
      elsif ch == "'" && in_quote
        in_quote = false; current += ch
      elsif ch == ',' && !in_quote
        values << current.strip; current = ''
      else
        current += ch
      end
    end
    values << current.strip unless current.strip.empty?
    values
  end

  # SQL value → Ruby literal  e.g.  '42' → 42,  'Alice' → "Alice"
  def ruby_literal(val)
    val = val.strip
    return val.to_i.to_s   if val =~ /\A\d+\z/
    return val              if val =~ /\A\d+\.\d+\z/
    return val.downcase     if %w[true false].include?(val.downcase)
    return 'nil'            if val.casecmp('null') == 0
    # Quoted SQL string: 'text'  →  "text"
    return "\"#{$1}\""     if val =~ /\A'(.*)'\z/m
    # SQL function calls: NOW(), CURRENT_TIMESTAMP, NEXTVAL('seq'), UUID()
    return val              if val =~ /\A\w+\(.*\)\z/m
    return val              if val =~ /\ACURRENT_/i
    # Negative number: -42
    return val              if val =~ /\A-\d+(\.\d+)?\z/
    # Fallback: return as-is (let the developer adjust)
    val
  end

  def sym_or_str(col)
    col =~ /\A\w+\z/ ? ":#{col}" : "\"#{col}\""
  end

  def condition_string_for(cond)
    condition_string(cond)
  end

  def finalize(chain)
    chain
  end
end
