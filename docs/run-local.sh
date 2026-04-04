PATH="/opt/homebrew/opt/ruby@3.2/bin:$PATH" \
  BUNDLE_PATH=vendor/bundle \
  RUBYOPT="-r./_plugins/ruby_compat.rb" \
  bundle exec jekyll serve --host 127.0.0.1 --port 4000
