require 'socket'
require 'json'

class Node

  def initialize(id, ip, port)
    @id = id
    @ip = ip
    @port = port
    @is_active = 1
    @routing_table = Hash.new
    @link_tuple = Struct.new(:url, :count)
    @index_tuple = Struct.new(:word, :link_tuple_array)
    @node_index = @index_tuple.new
    @is_gateway_node = false
    @message_buffer = Array.new
  end
  def get_id
    @id
  end
  def get_ip
    @ip
  end
  def get_port
    @port
  end
  def get_active
    @is_active
  end
  def get_route_table
    @routing_table
  end
  def get_gateway_ident
    @is_gateway_node
  end
  def get_index_table
    @node_index
  end
  def get_buffer
    @message_buffer
  end
  def set_id(data) @id = data end
  def set_ip(data) @ip = data end
  def set_port(data) @port = data end
  def set_active(data) @is_active = data end
  def set_gateway(data) @is_gateway_node = data end

  def add_to_routing_table(ident, ip_addr)#Maps nodes ids to their ip addresses
    @routing_table[ident] = ip_addr
    puts "Node #{ident} added to routing table"
  end

  def remove_from_routing_table(node_id)#Remove node from routing table
    if @routing_table.has_key?(node_id)
      @routing_table.delete(node_id)
      puts "Node #{node_id} removed from routing table"
    else puts 'Node id not present in routing table'
    end
  end

  def merge_routing_table(route_table)#Takes routing table and merges it with own
    if route_table.empty?
      puts 'Nothing to merge route table with'
    else
      @routing_table = @routing_table.merge(route_table[0])
    end
  end

  def find_closest_node(new_node_id)                  #Find node in routing table that is closest numerically with param
    return_node = {$node.get_id => $node.get_ip}      #Default to own node
    min = ($node.get_id.to_i - new_node_id.to_i).abs
    @routing_table.each_key { |key|                   #loop through keys in routing table
      difference = (key.to_i - new_node_id.to_i).abs  #Find difference between each key and param
      if difference < min
        min = difference                              #Keep record of which key has smallest difference
        return_node = {key => @routing_table[key]}
      end
    }
    return_node                                       #Return key,value pair with smallest difference
  end

  def index_word(keyword, links)                            #Takes keyword and links and creates a tuple
    if @node_index.word == nil                              #If index is not created, generate with passed in keyword
      puts 'Instantiating node index'
      @node_index = @index_tuple.new(keyword, [])
    end
    links.each { | param_item|                              #Cycle through passed in links
      flag = 0
      @node_index.link_tuple_array.each { |index_item|      #For each link passed in, go through each index entry
        if index_item.url == param_item                     #If link is already present in index
          index_item.count = index_item.count + 1           #Increment index element
          flag = 1                                          #Set flag to ensure that duplicates aren't added into index
        end
      }
      if flag == 0                                          #If link is not in index
        param_link_tuple = @link_tuple.new(param_item, 1)   #Create new entry
        @node_index.link_tuple_array.push(param_link_tuple) #And insert it into index
      end
    }
  end

  def hashCode(str)# Hashing Function
    hash = 0
    str.each_char {|c|
      hash = hash * 31 + c.ord
    }
    (hash).abs
  end


  def init(udp)# Function to initialise a node, passes in a param of type UDPSocket
    udp.bind($node.get_ip, $node.get_port)
  end

  def joinNetwork(bootstrap_node)                                                        #Sends a joining network message to node and receives a network id
    message = generate_message('JOINING_NETWORK', bootstrap_node.get_id, $node.get_id,
                               $node.get_ip, [], 0, 0, 0, 0, 0, 0)                       #generate join network message
    $u1.send message, 0, bootstrap_node.get_ip, bootstrap_node.get_port                  #send as udp message
    puts 'Sending join network message to bootstrap node' + message
    rand(2**32)                                                                          #return random number to serve as node id
  end

  def leaveNetwork(node_id)                                                   #Sends a message to all nodes in the routing table so they can remove node from their routing tables
    message = generate_message('LEAVING_NETWORK',0,node_id,0,[],0,0,0,0,0,0)
    route_table = $node.get_route_table
    ip_addresses = route_table.values
    ip_addresses.each do |node|
      $u1.send message, 0, node, OPERATING_PORT #send routing info            #For each node in routing table send message
    end
    true
  end

  def indexPage(url, word_set)                                                #Takes a url and an array of words and tells a node to index them
    puts 'Sending index messages now now'
    word_set.each do |word|                                                   #Send index messages out for each word
      hashed_word = $node.hashCode(word)                                      #target_id is hash of word
      message = generate_message('INDEX', 0, 0, 0, [], hashed_word, $node.get_id, word, [url], 0, 0)
      $u1.send message, 0, hashed_word, OPERATING_PORT
    end
    puts 'INDEX messages sent successfully'
  end

  def search(word_set)                                      #Send search messages to nodes based on an array of words and await responses
    word_set.each do |word|                                 #Send message to specific nodes based on hash of word
      hashed_word = $node.hashCode(word)
      message = generate_message('SEARCH', 0, hashed_word, 0, [], hashed_word, $node.get_id, '', [], word, '')
      $u1.send message, 0, hashed_word, OPERATING_PORT
    end
    sr_array = Array.new                                    #Initiate array to hold search results
    now = Time.now                                          #Start timer
    counter = 1
    loop do
      if Time.now < now + counter
        next
      else
        puts 'counting another second ...'                  #If message at head of buffer is a search response then grab it and put it into array
        if $node.get_buffer.length > 0 and $node.get_buffer[0]['type'] == 'SEARCH_RESPONSE'
          sr_array.push(receive_message($node.get_buffer[0]))
        else
          puts 'Waiting for search responses...'
        end
      end
      counter += 1
      break if counter > 3                                  #Wait for three seconds
    end
    puts 'Returning results'
    sr_array                                                #Return array of search results
  end
end

class SearchResult
  def initialize(words, url, freq)
    @words = words
    @url = url
    @frequency = freq
  end
  def get_words
    @words
  end
  def get_url
    @url
  end
  def get_freq
    @frequency
  end
  def set_words(data) @words = data end
  def set_url(data) @url = data end
  def set_freq(data) @frequency = data end
end



# Function that takes a string and uses that to determine the type of message to encode into JSON
def generate_message(message_type, gateway_id, node_id, ip_address, route_table, target_id, sender_id,
    keyword, link, word, response)
  result = ''
  case message_type

    when 'JOINING_NETWORK' then
      result = JSON.generate({type: message_type, node_id: node_id.to_s, ip_address: ip_address.to_s})

    when 'JOINING_NETWORK_RELAY' then
      result = JSON.generate({type: message_type, node_id: node_id.to_s, gateway_id: gateway_id.to_s})

    when 'ROUTING_INFO' then
      result = JSON.generate({type: message_type, gateway_id: gateway_id.to_s, node_id: node_id.to_s,
                              ip_address: ip_address, route_table: route_table})

    when 'LEAVING_NETWORK' then
      result = JSON.generate({type: message_type, node_id: node_id.to_s})

    when 'INDEX' then
      result = JSON.generate({type: message_type, target_id: target_id.to_s, sender_id: sender_id.to_s,
                              keyword: keyword.to_s, link: link})

    when 'SEARCH' then
      result = JSON.generate({type: message_type, word: word.to_s, node_id: node_id.to_s, sender_id: sender_id.to_s})

    when 'SEARCH_RESPONSE' then
      result = JSON.generate({type: message_type, word: word.to_s, node_id: node_id.to_s, sender_id: sender_id.to_s,
                              response: response})

    when 'PING' then
      result = JSON.generate({type: message_type, target_id: target_id.to_s, sender_id: sender_id.to_s,
                              ip_address: ip_address.to_s})

    when 'ACK' then
      result = JSON.generate({type: 'ACK', node_id: node_id.to_s, ip_adress: ip_address.to_s})

    else puts "Error - Unknown message type #{message_type}"
  end
  result
end

def receive_message(decoded_packet)#Act according to type of message received via decoded_packet
  case decoded_packet['type']#Start appropriate action

    when 'JOINING_NETWORK' then #send routing info to new node and forwards relay ot other nodes
      $node.set_gateway(true)
      $joining_node = {decoded_packet['node_id'] => decoded_packet['node_ip']}  #Record new node details for use later
      new_node_reply = generate_message('ROUTING_INFO',$node.get_id, decoded_packet['node_id'],$node.get_ip,$node.get_route_table,0,0,0,0,0,0)
      puts 'Sending Route information to joining node'
      $u1.send new_node_reply, 0, decoded_packet['ip_address'], OPERATING_PORT  #send routing info
      closest_node = $node.find_closest_node(decoded_packet['node_id'])         #Find node in routing table closest to joining node's id
      join_net_msg = generate_message('JOINING_NETWORK_RELAY',$node.get_id,decoded_packet['node_id'],0,[],0,0,0,0,0,0)
      puts 'Forwarding network relay onto other nodes'                          #Forward relay onto other nodes
      $u1.send join_net_msg, 0, closest_node.values[0], OPERATING_PORT

    when 'ROUTING_INFO' then  #Merge own tables with input and if gateway node, forward on to new node
      puts 'Routing Info message received, merging tables'                      #merge route table with own
      $node.merge_routing_table(decoded_packet['route_table'])
      if $node.get_gateway_ident                                                #If gateway node, send this to joining node
        route_msg = generate_message('ROUTING_INFO', $node.get_id, $joining_node.get_id, $node.get_ip, $node.get_route_table,0,0,0,0,0,0)
        puts 'Sending Routing information from other nodes onto joining node'
        $u1.send route_msg, 0, $joining_node.get_ip, OPERATING_PORT             #Send routing table onto joining node
      end

    when 'JOINING_NETWORK_RELAY' then #Find closest node and send relay on
      closest_node = $node.find_closest_node(decoded_packet['node_id'])         #Checks routing table for closest node
      if closest_node.keys[0] != $node.get_id                                   #If closest node is self then don't forward message to self
        join_net_msg = generate_message('JOINING_NETWORK_RELAY',decoded_packet['gateway_id'],decoded_packet['node_id'],0,[],0,0,0,0,0,0)#Generate message to pass on
        puts 'Passing network relay onto other nodes'
        $u1.send join_net_msg, 0, closest_node.values[0], OPERATING_PORT        #Pass message onto closest node
      end
      route_msg = generate_message('ROUTING_INFO', decoded_packet['gateway_id'], decoded_packet['gateway_id'], $node.get_ip, $node.get_route_table,0,0,0,0,0,0)
      puts 'Sending routing information to gateway node for joining node'       #Send routing info to gateway node
      $u1.send route_msg, 0, $node.get_route_table[decoded_packet['gateway_id']], $node.get_port

    when 'INDEX' then                                                           #Index work link tuple
      puts 'Indexing word-link tuples passed in'
      $node.index_word(decoded_packet['keyword'], decoded_packet['links'])
      ack_message = generate_message('ACK', 0, decoded_packet['sender_id'], $node.get_ip, [], 0, 0, 0, 0, 0, 0)#Send Acknowledgement
      closest_node = $node.find_closest_node(decoded_packet['sender_id'])         #Checks routing table for closest node
      $u1.send ack_message, 0, closest_node.values[0], OPERATING_PORT             #Sends ack over the network

    when 'SEARCH' then                                                          #Construct SEARCH_RESPONSE MESSAGE (If no results then send a null message)
      index_results = $node.get_index_table                                     #Get results from index
      search_response_msg = generate_message('SEARCH_RESPONSE', 0, decoded_packet['node_id'], 0, [], 0, decoded_packet['sender_id'], 0, 0, decoded_packet['word'], index_results.link_tuple_array)
      $u1.send search_response_msg, 0, $node.get_route_table[decoded_packet['sender_id'], $node.get_port]#Send message

    when 'SEARCH_RESPONSE' then                                                 #get searchresult from message and return to search function

      sr = SearchResult.new(decoded_packet['word'], decoded_packet['response'][0]['url'], decoded_packet['response'][0]['count'])
      puts "Returning search result #{sr}"
      sr

    when 'LEAVING_NETWORK' then
      $node.remove_from_routing_table(decoded_packet['node_id'])#Remove node from routing table

    when 'PING' then
      ack_msg = generate_message('ACK',0, decoded_packet['target_id'], decoded_packet['ip_address'], [], 0,0,0,0,0,0)#Generate ack
      $u1.send ack_msg, 0, decoded_packet['ip_address'], $node.get_port #Send to node who sent ping
      if $node.get_id != decoded_packet['target_id']#Check if node is final target
        ping_msg = generate_message('PING', 0, 0, $node.get_ip, [], decoded_packet['target_id'], decoded_packet['sender_id'], 0, 0, 0, 0)
        closest_node = $node.find_closest_node(decoded_packet['target_id'])
        $u1.send ping_msg, 0, closest_node.values[0], $node.get_port
      end

    when 'ACK' then
      if decoded_packet['node_id'] == $node.get_id                        #If ack has arrived at intended node then notify and continue
        puts 'ACK received'
        return true
      else
        ack_message = generate_message('ACK', 0, decoded_packet['node_id'], $node.get_ip, [], 0, 0, 0, 0, 0, 0)
        closest_node = $node.find_closest_node(decoded_packet['node_id']) #Find closest node to target and send message on
        $u1.send ack_message, 0, closest_node, $node.get_port
      end

    else 'Unknown message type received'
  end
end

def send_message(message_type, url, word_set, node_id)

  case message_type

    when 'JOINING_NETWORK' then
     puts 'Sending message to join network'
     $node.joinNetwork($gateway_node.get_ip) #Send joining network message to gateway node

    when 'LEAVING_NETWORK' then
      puts 'Leaving Network'
      success = $node.leaveNetwork($node.get_id)#Send leaving network message to all nodes in routing table
      if success
        puts 'Node has left the network successfully'
        $node.set_active(0)
      end

    when 'INDEX' then
      puts "Indexing #{word_set} with #{url}"
      $node.indexPage(url, word_set)#send index messages around network for each word passed in, target_id == hash of word

    when 'SEARCH' then
      puts "Searching for #{word_set}"
      search_results = $node.search(word_set)#send search messages around network for each word passed in, target_id == hash of word
      puts "Search results: #{search_results}"
    #search responses should then be received. After 3 seconds these are aggregated and a response returned. After 30s any non-received responses should cause pings to be sent

    when 'ACK' then                                   #Send in response to messages
      ack_message = generate_message('ACK', 0, node_id, $node.get_ip, [], 0, 0, 0, 0, 0, 0)
      closest_node = $node.find_closest_node(node_id) #Find closest node to target and ack
      $u1.send ack_message, 0, closest_node, $node.get_port

    else 'Unknown Message type'

  end
end

def analyse_arguments #Examine any command line arguments passed in and act accordingly
  ARGV.each { |arg|
    if arg.include? '--bootstrap'
      puts 'bootstrap argument detected'
      $gateway_node_ip = arg.slice(12, arg.length-1).to_i
    else
      if arg.include? '--boot'  #If boot argument detected then set node to gateway id
        puts 'Boot argument detected!!'
        $node.set_gateway(true)
        $node.set_id(arg.slice(7,arg.length-1).to_i)  #Assign node network id
      end
    end
    if arg.include? '--id'
      puts 'id argument detected'
      $gateway_node_id = arg.slice(5, arg.length-1).to_i
    end
  }
end



OPERATING_PORT = 8767
$node = Node.new(0,'localhost', OPERATING_PORT)
$u1 = UDPSocket.new
$node.init($u1)

analyse_arguments #Analyse any command line arguments passed in


t1 = Thread.new do

  while $node.get_active == 1                               #Listening Block
    packet = $u1.recvfrom(1024)                             #Get messages
    puts 'Packet Received, decoding JSON now'
    decoded_packet = JSON.parse(packet[0])                  #Decode messages
    puts "Packet decoded, type is #{decoded_packet['type']}"
    $node.get_buffer.push(decoded_packet)                   #Add message to buffer
    if $node.get_buffer[0]['type'] != 'SEARCH_RESPONSE'     #Do not act for search responses, this is dealt with in the search function
      receive_message($node.get_buffer[0])                  #Act differently depending on type of message
    end
  end
end

t2 = Thread.new do

  while $node.get_active == 1    #Input Block
    url = ''
    words = ''
    puts 'Enter Message Type to send: '
    input = gets.chomp
    puts "Input is: #{input}"
    if input == 'INDEX'
      puts 'Enter url:'
      url = gets.chomp
    end
    if input == 'INDEX' or input == 'SEARCH'
      puts 'Enter words separated by a comma:'
      words = gets.chomp
    end
    send_message(input, url, words.split(','), 0)
  end
end


t1.join
t2.join