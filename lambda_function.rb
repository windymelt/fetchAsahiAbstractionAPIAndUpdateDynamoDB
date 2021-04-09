require 'rubygems'
require "bundler/setup"
Bundler.require
require 'json'
require 'aws-sdk-dynamodb'
require 'net/http'
require 'time'

TABLE_NAME = 'asahi_abstraction_status'

def lambda_handler(event:, context:)
  dynamodb = Aws::DynamoDB::Client.new(region: 'ap-northeast-1')
  event['Records'].each do |record|
    return { statusCode: 200, body: JSON.generate('circuit breaker is open') } unless check_circuit(dynamodb: dynamodb)
    url = record['body']
    puts url
    article = fetch_article(url: url)
    puts "article:"
    puts article
    begin
      abstract = send_to_abstraction_api(text: article)
      puts "generated abstract:"
      puts abstract
      update_dynamodb(dynamodb: dynamodb, url: url, status: "done", abstract: abstract)
    rescue => err
      puts err
      break_circuit(dynamodb: dynamodb)
      return { statusCode: 200, body: JSON.generate('Ignoring due to too many requests') }
    end
  end

  { statusCode: 200, body: JSON.generate('Done') }
end

def fetch_article(url:)
  uri = URI(url)
  puts "fetching"
  html = Net::HTTP.get(uri)
  puts "fetch done"
  doc = Nokogiri::HTML.parse(html, nil, "utf-8")
  article = doc.css('div.entry-content').inner_text
  return article
end

def send_to_abstraction_api(text:)
  uri = URI("https://clapi.asahi.com/control-len")
  api_key = ENV['API_KEY']
  headers = { "Content-Type" => "application/json", "x-api-key" => api_key, "accept" => "application/json" }
  params = { text: text, length: 500, auto_paragraph: true }
  res = Net::HTTP::post(uri, params.to_json, headers)
  if (res.code != "200")
    raise res
  end
  json = JSON.parse(res.body)
  return json['result'].join('▼')
end

def update_dynamodb(dynamodb:, url:, status:, abstract:)
  params = {
    table_name: TABLE_NAME, 
    key: { url: url },
    update_expression: 'set abstract = :ab, #st_col = :st',
    # 'status' is reserved keyword.
    # we pass 'status' as attribute values to avoid this error
    expression_attribute_names: { '#st_col': 'status' },
    expression_attribute_values: { ':ab': abstract, ':st': status },
    return_values: 'UPDATED_NEW'
  }
  begin
    dynamodb.update_item(params)
  rescue Aws::DynamoDB::Errors::ServiceError => error
    puts "Unable to set status:"
    puts error.message
  end
end

def check_circuit(dynamodb:)
  params = {
    table_name: TABLE_NAME,
    key: {
      url: 'CIRCUIT_BREAKER'
    }
  }
  result = dynamodb.get_item(params)

  if result.item == nil
    puts 'no circuit breaker saved'
    return true
  end

  oktime = Time.iso8601(result.item['status'])
  now = Time.now()

  return now > oktime
end

def break_circuit(dynamodb:)
  nextday = Time.now() + 60*60*24
  params = {
    table_name: TABLE_NAME, 
    key: { url: 'CIRCUIT_BREAKER' },
    update_expression: 'set #st_col = :st',
    # 'status' is reserved keyword.
    # we pass 'status' as attribute values to avoid this error
    expression_attribute_names: { '#st_col': 'status' },
    expression_attribute_values: { ':st': nextday.to_s },
    return_values: 'UPDATED_NEW'
  }
  begin
    dynamodb.update_item(params)
  rescue Aws::DynamoDB::Errors::ServiceError => error
    puts "Unable to set status:"
    puts error.message
  end
end
