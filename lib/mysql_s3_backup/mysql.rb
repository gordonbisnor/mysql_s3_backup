module MysqlS3Backup
  class Mysql
    include Shell
    
    attr_reader :database, :bin_log_path
    
    def initialize(options)
      options = options.symbolize_keys
      @user = options[:user] || raise(ArgumentError, "user required")
      @password = options[:password]
      @database = options[:database] || raise(ArgumentError, "database required")
      @bin_log_path = options[:bin_log]
      @bin_path = options[:bin_path]
    end
    
    def cli_options
      cmd = "-u'#{@user}'"
      cmd += " -p'#{@password}'" if @password
      cmd += " #{@database}"
      cmd
    end
    
    def execute(sql)
      run %{#{@bin_path}mysql -e "#{sql}" #{cli_options}}
    end
    
    def execute_file(file)
      run "cat '#{file}' | #{@bin_path}mysql #{cli_options}"
    end
    
    def dump(file)
      cmd = "#{@bin_path}mysqldump --quick --single-transaction --create-options -u'#{@user}'"
      cmd += " --flush-logs --master-data=2 --delete-master-logs" if @bin_log_path
      cmd += " -p'#{@password}'" if @password
      cmd += " #{@database} | gzip > #{file}"
      run cmd
    end
    
    def restore(file)
      run "gunzip -c #{file} | #{@bin_path}mysql #{cli_options}"
    end
    
    def get_log_list
      execute("show binary logs").            # get the result
        split("\n")[1..-1].                   # remove the header
        map { |line| line.split("\t").first } # return only the filename
    end
    
    def each_bin_log
      execute "flush logs"
      logs = get_log_list
      logs_to_archive = logs[0..-2] # all logs except the last
      logs_to_archive.each do |log|
        yield File.join(File.dirname(@bin_log_path), log)
      end
      execute "purge master logs to '#{logs[-1]}'"
    end
    
    def apply_bin_log(file)
      cmd = "#{@bin_path}mysqlbinlog --database=#{@database} #{file} | mysql -u#{@user} "
      cmd += " -p'#{@password}' " if @password
      run cmd
    end
  end
end