
require 'optparse'

$:.unshift File.expand_path('../../lib', __FILE__)
require 'nats/client'

$count = 100000
$batch = 100

$delay = 0.00001
$dmin  = 0.00001

TRIP  = (2*1024*1024)
TSIZE = 4*1024

$sub  = 'test'
$data_size = 16
$pool = 8

$hash  = 2500

STDOUT.sync = true

parser = OptionParser.new do |opts|
  opts.banner = "Usage: pub_perf [options]"

  opts.separator ""
  opts.separator "options:"

  opts.on("-n COUNT",   "Messages to send (default: #{$count}}") { |count| $count = count.to_i }
  opts.on("-s SIZE",    "Message size (default: #{$data_size})") { |size| $data_size = size.to_i }
  opts.on("-S SUBJECT", "Send subject (default: (#{$sub})")      { |sub| $sub = sub }
  opts.on("-b BATCH",   "Batch size (default: (#{$batch})")      { |batch| $batch = batch.to_i }
end

parser.parse(ARGV)

trap("TERM") { exit! }
trap("INT")  { exit! }

NATS.on_error { |err| puts "Error: #{err}"; exit! }

$data = Array.new($data_size) { "%01x" % rand(16) }.join('').freeze

NATS.start(
  :fast_producer_error => true,
  servers: ["nats://localhost:4222", "nats://localhost:5222", "nats://localhost:6222"],
  dont_randomize_servers: true,
) do

  $start   = Time.now
  $to_send = $count

  $batch = 10 if $data_size >= TSIZE

  def send_batch
    (0..$batch).each do
      $to_send -= 1
      if $to_send <= 0
        NATS.publish($sub, $data) { display_final_results }
        return
      else
        NATS.publish($sub, $data)
      end
      printf('+') if $to_send.modulo($hash) == 0
    end

    # if (NATS.pending_data_size > TRIP)
    #   $delay *= 2
    # elsif $delay > $dmin
    #   $delay /= 2
    # end

    send_batch
    # EM.add_timer($delay) { send_batch }
  end

  if false
    EM.add_periodic_timer(0.25) do
      puts "Outstanding data size is #{NATS.client.get_outbound_data_size}"
    end
  end

  puts "Sending #{$count} messages of size #{$data.size} bytes on [#{$sub}]"

  def display_final_results
    if NATS.client.get_outbound_data_size > 0
      puts "pending data size = #{NATS.pending_data_size}: delaying results"
      puts "outbound data size = #{NATS.client.get_outbound_data_size}: delaying results"
      EM.next_tick { display_final_results }
    else
      elapsed = Time.now - $start
      mbytes = sprintf("%.1f", (($data_size*$count)/elapsed)/(1024*1024))
      puts "\nTest completed : #{($count/elapsed).ceil} msgs/sec (#{mbytes} MB/sec)\n"
      puts "pending data size = #{NATS.pending_data_size}"
      puts "outbound data size = #{NATS.client.get_outbound_data_size}"
      puts "pool = #{$pool}"
      EM.next_tick { NATS.stop }
    end
  end

  def wait_for_threads(threads)
    alive = threads.count { |t| t.alive? }

    if alive > 0
      #puts "waiting for #{alive} threads: #{NATS.pending_data_size}, #{NATS.client.get_outbound_data_size}"
      # threads.each { |t| puts t.backtrace.join("\n") if t.alive? }
      EM.add_timer(alive/$pool) { wait_for_threads(threads) }
    else
      puts "threads finished"
      display_final_results
    end
  end

  # kick things off..
  total_msgs = $count
  threads = $pool.times.map do
    Thread.new do
      (total_msgs/$pool).times do
        if NATS.client.get_outbound_data_size > 1024**2
          sleep 0.01 until NATS.client.get_outbound_data_size < 1024
        end
        NATS.publish($sub, $data)
      end
    end
  end

  EM.next_tick { wait_for_threads(threads) }
end

