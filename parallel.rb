# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "set"

class LogStash::Filters::Parallel < LogStash::Filters::Base

	config_name "parallel"
	milestone 1

	config :start, :validate => :string, :required => true

	config :end, :validate => :string, :required => true

	config :stream_identity, :validate => :string, :default => "%{uuid}"

	config :patterns_dir, :validate => :array, :default => []


	@@patterns_path = Set.new
	  if __FILE__ =~ /file:\/.*\.jar!.*/
 	   @@patterns_path += ["#{File.dirname(__FILE__)}/../../patterns/*"]
	  else
 	   @@patterns_path += ["#{File.dirname(__FILE__)}/../../../patterns/*"]
	  end
	
	def initialize(config = {})
		super
		@threadsafe = false
		@types = Hash.new { |h,k| h[k] = [] }
		@pending = Hash.new

	end # def initialize
	  	
  	def register
    	require "grok-pure" # rubygem 'jls-grok'

    	@grok = Grok.new
    	@grok2 = Grok.new

    	@patterns_dir = @@patterns_path.to_a + @patterns_dir
    	@patterns_dir.each do |path|
      		# Can't read relative paths from jars, try to normalize away '../'
      		while path =~ /file:\/.*\.jar!.*\/\.\.\//
        		# replace /foo/bar/../baz => /foo/baz
        		path = path.gsub(/[^\/]+\/\.\.\//, "")
     		 end

      		if File.directory?(path)
        		path = File.join(path, "*")
      		end

      		Dir.glob(path).each do |file|
        		@logger.info("Grok loading patterns from file", :path => file)
        		@grok.add_patterns_from_file(file)
      		end
    	end

    	@grok.compile(@start)
	    @grok2.compile(@end)

	end # def register

	public
  def filter(event)
    return unless filter?(event)

    if event["message"].is_a?(Array)
      match_start = @grok.match(event["message"].first)
      match_end =@grok2.match(event["message"].first)
    else
      match_start = @grok.match(event["message"])
      match_end = @grok2.match(event["message"])
    end
    key = event.sprintf(@stream_identity)
    pending = @pending[key]

    @logger.debug("parallel", :start => @start, :end => @end, :message => event["message"])
    if match_end
    	pending.append(event)
    	tmp = event.to_hash
        event.overwrite(pending)
  		
    else
    	if match_start
            #event.overwrite(pending)
            #pending = true
            tmp = event.to_hash
            @pending[key] = LogStash::Event.new(tmp)
            event.cancel
    		
    	else
    		event.tag "parallel"
    		if pending
        		pending.append(event)
        	else
          		@pending[key] = event
       		end
        	event.cancel
    	end
    end

    if !event.cancelled?
      collapse_event!(event)
      filter_matched(event) if match_end
    end
  end # def filter

  private

  def collapse_event!(event)
    event["message"] = event["message"].join("\n") if event["message"].is_a?(Array)
    event["@timestamp"] = event["@timestamp"].first if event["@timestamp"].is_a?(Array)
    event
  end

end
