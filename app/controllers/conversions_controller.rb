# frozen_string_literal: true

class ConversionsController < ApplicationController
  layout false

  def index
    @examples = conversion_examples
  end

  def convert
    input     = params[:input].to_s.strip
    direction = params[:direction].to_s

    if input.blank?
      return render json: { error: "Please enter a query to convert." }, status: :unprocessable_entity
    end

    output = case direction
             when 'sql_to_ar'
               SqlToActiverecordConverter.new(input).convert
             when 'ar_to_sql'
               ActiverecordToSqlConverter.new(input).convert
             else
               return render json: { error: "Unknown conversion direction." }, status: :bad_request
             end

    render json: { output: output }

  rescue SqlToActiverecordConverter::ConversionError,
         ActiverecordToSqlConverter::ConversionError => e
    render json: { error: e.message }, status: :unprocessable_entity
  rescue => e
    render json: { error: "Unexpected error: #{e.message}" }, status: :internal_server_error
  end

  private

  def conversion_examples
    [
      {
        label: "Simple SELECT",
        direction: "sql_to_ar",
        input: "SELECT * FROM users WHERE active = 1 ORDER BY name ASC LIMIT 10",
        output: "User.where(active: 1).order(:name).limit(10)"
      },
      {
        label: "Multi-condition WHERE",
        direction: "sql_to_ar",
        input: "SELECT * FROM products WHERE price > 100 AND category = 'electronics'",
        output: "Product.where(\"price > ?\", 100).where(category: \"electronics\")"
      },
      {
        label: "INNER JOIN",
        direction: "sql_to_ar",
        input: "SELECT * FROM users INNER JOIN posts ON posts.user_id = users.id WHERE users.active = 1",
        output: "User.where(active: 1).joins(:posts)"
      },
      {
        label: "GROUP BY + HAVING + COUNT",
        direction: "sql_to_ar",
        input: "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id HAVING COUNT(*) > 5",
        output: "Order.select(:user_id).group(:user_id).having(\"COUNT(*) > 5\").count"
      },
      {
        label: "INSERT",
        direction: "sql_to_ar",
        input: "INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)",
        output: "User.create(name: \"Alice\", email: \"alice@example.com\", age: 30)"
      },
      {
        label: "UPDATE with WHERE",
        direction: "sql_to_ar",
        input: "UPDATE users SET active = 0 WHERE age < 18",
        output: "User.where(\"age < ?\", 18).update_all(active: 0)"
      },
      {
        label: "DELETE",
        direction: "sql_to_ar",
        input: "DELETE FROM sessions WHERE user_id = 42",
        output: "Session.where(user_id: 42).destroy_all"
      },
      {
        label: "AR → SQL (Basic WHERE)",
        direction: "ar_to_sql",
        input: "User.where(active: true).order(name: :asc).limit(10)",
        output: "SELECT \"users\".* FROM \"users\" WHERE \"users\".\"active\" = TRUE ORDER BY \"users\".\"name\" ASC LIMIT 10;"
      },
      {
        label: "AR → SQL (String condition)",
        direction: "ar_to_sql",
        input: "User.where(\"age > ?\", 18).select(:id, :name)",
        output: "SELECT \"users\".\"id\", \"users\".\"name\" FROM \"users\" WHERE (age > 18);"
      },
      {
        label: "AR → SQL (JOIN + COUNT)",
        direction: "ar_to_sql",
        input: "Order.joins(:products).group(:user_id).count",
        output: "SELECT COUNT(*) FROM \"orders\" INNER JOIN \"products\" ON \"products\".\"order_id\" = \"orders\".\"id\" GROUP BY \"orders\".\"user_id\";"
      },
      {
        label: "AR → SQL (INSERT)",
        direction: "ar_to_sql",
        input: "User.create(name: \"Bob\", email: \"bob@example.com\")",
        output: "INSERT INTO \"users\" (\"name\", \"email\") VALUES ('Bob', 'bob@example.com');"
      },
      {
        label: "AR → SQL (DELETE)",
        direction: "ar_to_sql",
        input: "Session.where(user_id: 42).destroy_all",
        output: "DELETE FROM \"sessions\" WHERE \"sessions\".\"user_id\" = 42;"
      }
    ]
  end
end
