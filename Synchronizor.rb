#encoding: utf-8
require "yaml"
require "find"
require "digest"
require "fileutils"
require "monitor"
require "logger"

MovedContainer = Hash.new
MovedContainer.extend(MonitorMixin)

if ENV['OS'].upcase.include?("WINDOWS")
  FILE_SEPARATOR = File::ALT_SEPARATOR
else
  FILE_SEPARATOR = File::SEPARATOR
end 

parsed = begin
  root =  File.dirname(__FILE__)
  YAML.load(File.open(root+"\\config.yml"))
rescue ArgumentError => e
  puts "Could not parse YAML: #{e.message}"
end


WORKING_DIR = parsed["working_dir"]
DEST_DIR = parsed["destination"] 
DEST_DIR << FILE_SEPARATOR unless DEST_DIR.end_with?(FILE_SEPARATOR)
UNCHANGABLE_EXT = parsed["unchangable.ext"].split(/ /);
FILTER_EXT = parsed["filter.ext"].split(/ /);

if parsed["log.file"].nil? or parsed["log.file"] == ""
  Logfile = File.open(WORKING_DIR + FILE_SEPARATOR + 'stdout.log', "w")
else
  logdir = File.dirname parsed["log.file"]
  FileUtils.mkdir_p(logdir) unless FileTest.exist?(logdir) 
  Logfile = File.open(parsed["log.file"], "w")
end

FILE_LOGGER = Logger.new(Logfile,"daily")

def checksum(file)
  return Digest::SHA2.file(file).hexdigest
end

#Print the log both to console and log file
def logger(severity, message)
  puts message
  severity = "info" if severity.nil?
  case severity
    when "info" then
      FILE_LOGGER.info(message)
    when "error" then
      FILE_LOGGER.error(message)
    when "fatal" then
      FILE_LOGGER.fatal(message)
    when "warn" then
      FILE_LOGGER.fatal(message)
    when "debug" then
      FILE_LOGGER.debug(message)
    else
      #Do nothing
  end
end


def generateRecord(fullpath)
  if UNCHANGABLE_EXT.include?(File.extname(fullpath).downcase)
    record = removeDiskPrefix(fullpath)
  else
    record = removeDiskPrefix(fullpath) << "|" << checksum(fullpath)
  end
  return record
end
  

def parseDirectory(directory)
  Find.find(directory) do |path|
      if FileTest.directory?(path)
        if File.basename(path)[0] == "."
          Find.prune       
        else
          next
        end
      else 
        begin
          if FILTER_EXT.include? File.extname(path)
            logger("info","Filtered file: " << path )
            next 
          end
          yield generateRecord(path)
        rescue StandardError => e
          logger("info","Skipped file: " << path) 
          next
        end
      end
  end 
end

def cleanSlash(path)
  path = path.gsub(/:/,"") if path.include?(":")
  if ENV['OS'].upcase.include?("WINDOWS")
    return path.gsub(/\\/,".")
  else
    return path.gsub(/\//,".")
  end  
end

def getDiskPrefix(source)
  if source.include?(":")
    return source[0..source.index(FILE_SEPARATOR)]
  else
    raise 
  end
end

def removeDiskPrefix(path)
  if path.include?(":")
    return path[path.index(FILE_SEPARATOR)+1..path.length] 
  else
    return path
  end
end

def moveFile(source,record)
  pathFlatten = cleanSlash(source)
  diskPrefix = getDiskPrefix(source)
  if record.include?("|")
    file = diskPrefix << record.split("|")[0]
  else
    file = diskPrefix << record
  end
  file = file.chomp
  dir = File.dirname(file)
  begin
    destDir = DEST_DIR + removeDiskPrefix(dir)
    FileUtils.mkdir_p(destDir) unless FileTest.exist?(destDir) 
    logger("info","Moved " << file << " ==> " <<  destDir)
    FileUtils.cp_r(file, destDir)
    MovedContainer.synchronize do
        MovedContainer[pathFlatten] << record 
    end
  rescue StandardError => e
    logger("fatal","MoveFile - error: " << file << " " << e.message)
    MovedContainer.synchronize do
        MovedContainer[pathFlatten].delete file
    end
  end
end


def handleSingleFile(file)
  logger("info","Synchronize single file: " << file)
  dir = File.dirname(file)
  destDir = DEST_DIR + removeDiskPrefix(dir)
  destFile = destDir + File.basename(file)
  if FileTest.exist?(destFile)
    FileUtils.cp_r(file, destDir) unless checksum(file) == checksum(destFile)
  else
    FileUtils.mkdir_p(destDir)
    FileUtils.cp_r(file, destDir)
  end
end


def compareHistoryRecord(source)
  pathFlatten = cleanSlash(source)
  historyFile = WORKING_DIR + FILE_SEPARATOR + pathFlatten+".syn.moved"
  newRecordFile = WORKING_DIR + FILE_SEPARATOR + pathFlatten+".syn.log.new"
  diffRecord = []
  begin
    unless FileTest.exist?(historyFile)
      parseExisting(source)
    end
    
    newFile = File.open(newRecordFile, 'r')
    newRecords = newFile.readlines
    
    oldFile = File.open(historyFile, 'r') if FileTest.exist?(historyFile)
    if oldFile.nil?
       oldRecords = [] 
    else
       oldRecords = oldFile.readlines 
    end
    diffRecord = newRecords - oldRecords
    
  rescue StandardError => e
    logger("fatal", "CompareHistoryRecord - error: " << source << " " << e.message) 
    raise
  ensure
      newFile.close unless oldFile.nil?
      oldFile.close unless oldFile.nil?
  end
  return diffRecord
end

def parseExisting(source)
  pathPrefix = cleanSlash(source)
  dest = DEST_DIR + removeDiskPrefix(source)
  if FileTest.exist?(dest)
    logger("info","History moved records does not exist, now parseExisting: " << dest)
    begin
      File.open(WORKING_DIR + FILE_SEPARATOR + pathPrefix+".syn.moved", 'w') do |file| 
        parseDirectory(dest){|line| file.puts line}
      end 
    rescue StandardError => e
      logger("fatal", "ParseExisting - error: " << dest << " " << e.message)
    end
  end
end

def dumpMovedRecords(pathFlatten)
  MovedContainer.synchronize do
    if pathFlatten.nil?
      MovedContainer.keys.each do |key|
        File.open(WORKING_DIR + FILE_SEPARATOR + key+".syn.moved", 'a+') do |file| 
          MovedContainer[key].each{|record| file.puts record}
        end 
      end
    else
      File.open(WORKING_DIR + FILE_SEPARATOR + pathFlatten+".syn.moved", 'a+') do |file| 
        MovedContainer[pathFlatten].each{|record| file.puts record}
      end 
      MovedContainer.delete(pathFlatten)
    end
  end
end


begin
  threads = []
  for source in parsed["source"]
    if FileTest.directory?(source)
      threads << Thread.new(source) do |srcPath|
        logger("info","Parsing -- " << srcPath << "\n")
        diskPrefix = getDiskPrefix(srcPath)
        pathFlatten = cleanSlash(srcPath)
        MovedContainer.synchronize do
          MovedContainer[pathFlatten] = Array.new
        end
        newRecordFile = WORKING_DIR + FILE_SEPARATOR + pathFlatten+".syn.log.new"
        File.delete(newRecordFile) if FileTest.exist?(newRecordFile)
        File.open(newRecordFile, 'w') do |file| 
          parseDirectory(srcPath){|line| file.puts line}
        end 
        diffRecords = compareHistoryRecord(srcPath)
        if diffRecords.length == 0
          logger("info","Skipped " <<  srcPath << ". No file need to be synchronized.")
        end
        diffRecords.each{|record| moveFile(srcPath, record)}
        dumpMovedRecords(pathFlatten)
      end
     else
        handleSingleFile(source) #For single file
    end
  end
  threads.each{|thread| thread.join}
rescue StandardError => e
  logger("fatal","Main Thread - error: " << e.message )
  raise
ensure
  dumpMovedRecords(nil)
end
