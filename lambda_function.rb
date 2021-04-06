require 'rubygems'
require "bundler/setup"
Bundler.require
require 'json'
require 'aws-sdk-dynamodb'
require 'net/http'

TABLE_NAME = 'asahi_abstraction_status'

def lambda_handler(event:, context:)
  dynamodb = Aws::DynamoDB::Client.new(region: 'ap-northeast-1')
  event['Records'].each do |record|
    url = record['body']
    puts url
    article = fetch_article(url: url)
    puts "article:"
    puts article
    begin
      abstract = send_to_abstraction_api(text: article)
      update_dynamodb(dynamodb: dynamodb, url: url, status: "done", abstract: abstract)
    rescue => err
      puts err
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
  article = doc.css('article.entry').inner_text
  article = article.sub(/.*†††/m, '')
  return article
end

def send_to_abstraction_api(text:)
  uri = URI("https://clapi.asahi.com/control-len")
  api_key = ENV['API_KEY']
  headers = { "Content-Type" => "application/json", "x-api-key" => api_key, "accept" => "application/json" }
  params = { text: text, length: 500, auto_paragraph: true }
  html = Net::HTTP::post(uri, params.to_json, headers)
  json = JSON.parse(html)
  return json['result'].join('<br/>')
end

def update_dynamodb(dynamodb:, url:, status:, abstract:)
  params = {
    table_name: TABLE_NAME, 
    key: { url: url },
    update_expression: 'set abstract = :ab, status = :st',
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
