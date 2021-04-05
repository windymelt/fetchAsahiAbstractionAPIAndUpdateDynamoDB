.PHONY: build

build:
	docker run -v `pwd`:/var/task -it lambci/lambda:build-ruby2.7 bundle install --path vendor/bundle
	zip -r function.zip lambda_function.rb vendor Gemfile Gemfile.lock
