require 'aws-sdk'

module MysqlS3Backup
  class Bucket
    def initialize(name, options)
      @name = name
      @s3_options = options.symbolize_keys.merge(:use_ssl => true)
      connect
      create
    end
    
    def connect
      @s3 = AWS::S3.new(@s3_options)
    end
    
    def create
      # It doesn't hurt to try to create a bucket that already exists
      @bucket = @s3.buckets.create(@name)
    end
    
    def store(file_name, file)
      @object.write(open(file))
    end
    
    def copy(file_name, new_file_name)
      AWS::S3::S3Object.copy(file_name, new_file_name, @name)
    end
    
    def fetch(file_name, file)
      open(file, 'w') do |f|
        AWS::S3::S3Object.stream(file_name, @name) do |chunk|
          f.write chunk
        end
      end
    end
    
    def find(prefix)
      @object = @bucket.objects[@name]
    end
    
    def delete_all(prefix)
      AWS::S3::Bucket.objects(@name, :prefix => prefix).each { |obj| obj.delete }
    end
  end
end