#!/usr/bin/env ruby

require 'socket'
require 'time'

SYNC = true
ASYNC = false

NT = [ 0, 1, 0, 0, 1, 2, 4, 8, 4, 8, 1, 0, 0, 4, 4, 8, 0, 4, 4, 4 ]  #byte length of different datatypes

class Month
  def initialize(x)
    @i = x
  end

  def to_s
    m = @i + 24000
    y = m/12
    "#{y/100}-#{y%100}-#{(m+1)%12}"
  end
end

class Minute
  def initialize(x)
    @i = x
  end

  def to_s
    "#{@i/60}-#{@i%60}"
  end
end

class Second
  def initialize(x)
    @i = x
  end

  def to_s
    "#{@i/60}-#{@i%60}"
  end
end

class Dict
  def initialize(x,y)
    @x = x
    @y = y
    @length = @x.length
  end

  def each
    0.upto(@x.length - 1) do |i|
      yield @x[i],@y[i]
    end
  end 
end

class Flip

  def initialize(d)
    @x = Array.new
    @y = Array.new

    d.each do |k,v|
      @x.push(k)
      @y.push(v)
    end

    @length = @y[0].to_s.length
    @index = 0
  end
end 

# This is a function 
def td(x)

  if x.class == Flip 
    return x
  end
  if x.class != Dict
    raise 'This Function only takes a Dict type'
  end

  a = x.x
  b = x.y
  x = Array.new 
  for item in a.x do
    x.push item
  end
  for item in b.x do
    x.push item
  end
  y = Array.new
  for item in a.x do
    x.push item
  end
  for item in b.x do
    x.push item
  end

  return Flip.new Dict.new(x,y)
end


K = 8640000 * 10957
STDOFFSET = Time.now.utc_offset

class Qkdb
  
  @@reconnect_attempts = 5
  @@reconnect_wait = 5000
  @@max_msg_query_length = 1024
  @@max_msg_list_length = 100
  @little_endian = false

  def lg(x)
    return x + STDOFFSET
  end

  def gl(x)
    return x - STDOFFSET
  end
  def initialize(host, port, login)
    @host = host
    @port = port
    @login = login
    connect(host, port, login)
    @offset = 0
  end

  def connect(host,port,login)
    @sock = TCPSocket.open(host, port)
    begin
      @sock.send(login+"\x00", 0)
      result = @sock.recv(1) 
      if not result
        raise "Access Denied"
      end
    rescue 
      raise "Unable to connect to host"
    end

  end

  def k(query,args = nil)

    if query.class==String and args.nil?
      _send(SYNC, query)
    end
    #TODO : Do the writing part,i.e. args is nil 
    _recv_from_server
  end 

  def _send(sync, query)
    n = _nx(query) + 8
  
    if sync
      message = [0,1,0,0].pack("c*") 
    else
      message = [0,0,0,0].pack("c*") 
    end
    message += [n].pack("N")
    message += _write(query)
    @sock.send(message, 0)
  end

  def _recv_from_server
    header = @sock.recv(8)
    @little_endian = header[0].unpack('c')[0] == 1
    @offset = 4
    data_size = _ri(header);
    inputBytes = recv_size data_size - 8
    if inputBytes.unpack('b')[0] == -128
      @offset = 1
      raise "Faulty data reading #{inputBytes}"
    end
    @offset = 0
    _r(inputBytes)
  end 

  def recv_size(size)
    total_len = 0
    total_data = Array.new
    data_size = [8192,size].min
    begin
     sock_data = @sock.recv data_size
     total_data.push sock_data
     total_len += sock_data.length
    end while total_len < size 

    total_data.join('')
  end

  def _nx(x)
    qtype = _qtype(x)

    if qtype == 99
      return 1 + _nx(x.x) + _nx(x.y)
    end

    if qtype == 98
      return 3 + _nx(x.x) + _nx(x.y)
    end

    if qtype < 0
      if qtype == -11
        return 2 + x.length
      else
        return 1 + NT[-qtype]
      end
    end

    j = 6
    sn = n(x)
    if qtype == 0 or qtype == 11 
      n.times do 
        if qtype == 0
          j += _nx(x[i])
        else
          1 + x[i].length
        end
      end
    else
      j += sn * NT[qtype]
    end
    j
  end

  def _qtype(x)
    # implement this
    10
  end 

  def _wb(x)
    x.bytes.pack("b")
  end

  def _wi(x)
    x.bytes.pack("i>")
  end
  
  def _ws(x)
    val = x.bytes
    val += "\u0000"
    val
  end
  
  def _wc(x, message)
    x.bytes.pack("c*")
  end


  def n(x)
    x.length
  end 

  def _qtype(x)
    10
  end 
  def _write(x)
    #just writing chars for now
    size = n(x)
    message = [_qtype(x)].pack('s')
    message += [size].pack("i>")
    message += _wc(x,message)
    message
  end
  
  def _endian_decide(fmt)
    if @little_endian == true
       fmt
    else
      fmt+'>'
    end
  end

  def _rb(bytearray)
    val = bytearray[@offset].unpack("c")[0]
    @offset += 1
    val
  end
  
  def _rc(bytearray)
    val = bytearray[@offset].unpack("c")[0]
    @offset += 1
    val
  end

  def _ri(bytearray)
    val = bytearray[@offset..@offset+3].unpack(_endian_decide("i"))[0]
    @offset += 4
    val
  end

  def _rd(bytearray)
   val = bytearray[@offset..@offset+3].unpack(_endian_decide("i"))[0] 
   @offset += 4 
   val 
  end

  def _rt(bytearray)
    val = bytearray[@offset..@offset+3].unpack(_endian_decide("i"))[0]
    @offset += 4
    val
  end

  def _rdt(bytearray)
    val = bytearray[@offset..@offset+7].unpack(_endian_decide("d"))[0]
    @offset += 8
    val
  end
  
  def _re(bytearray)
    val = bytearray[@offset..@offset+3].unpack(_endian_decide("e"))[0]
    @offset += 4
    val
  end

  def _rj(bytearray)
    val = bytearray[@offset..@offset+7].unpack(_endian_decide("q"))[0]
    @offset += 8
    val
  end

  def _rf(bytearray)
    val = bytearray[@offset..@offset+7].unpack(_endian_decide("d"))[0]
    @offset += 8
    val
  end

  def _rh(bytearray)
    val = bytearray[@offset..@offset+1].unpack(_endian_decide("s"))[0]
    @offset += 2
    val
  end

  def _rs(bytearray)
    e = bytearray.index("\u0000",@offset)
    val = bytearray[@offset..e-1]
    @offset=e+1
    val
  end

  def _r(bytearray)

    t = _rb(bytearray)
    readType = {

      -1 => lambda { _rb(bytearray)},
      -4 => lambda { _rb(bytearray)},
      -5 => lambda { _rh(bytearray)},
      -6 => lambda { _ri(bytearray)},
      -7 => lambda { _rj(bytearray)},
      -8 => lambda { _re(bytearray)},
      -9 => lambda { _rf(bytearray)},
      -10 => lambda { _rc(bytearray)},
      -11 => lambda { _rs(bytearray)},
      -13 => lambda { Month.new _ri(bytearray)},
      -14 => lambda { _rd(bytearray)},
      -15 => lambda { _rdt(bytearray)},
      -17 => lambda { Minute _ri(bytearray)},
      -18 => lambda { Second _ri(bytearray)},
      -19 => lambda { _rt(bytearray)},
      0 => lambda { _r(bytearray)},
      1 => lambda { _rb(bytearray)},
      4 => lambda { _rb(bytearray)},
      5 => lambda { _rh(bytearray)},
      6 => lambda { _ri(bytearray)},
      7 => lambda { _rj(bytearray)},
      8 => lambda { _re(bytearray)},
      9 => lambda { _rf(bytearray)},
      10 => lambda { _rc(bytearray)},
      11 => lambda { _rs(bytearray)},
      13 => lambda { Month.new _ri(bytearray)},
      14 => lambda { _rd(bytearray)},
      15 => lambda { _rdt(bytearray)},
      17 => lambda { Minute _ri(bytearray)},
      18 => lambda { Second _ri(bytearray)},
      19 => lambda { _rt(bytearray)}

    }

    if t < 0
      if readType.has_key?(t)
        return readType[t].call
      end
    end

    if t > 99
      if t == 100
        _rs(bytearray)
        return _r(bytearray)
      end

      if t < 104
        if _rb(bytearray) == 0 and t == 101
          return nil
        else
          return "func"
        end
      end
      @offset = bytearray.length
      return "func"
    end
    if t == 99
      keys = _r(bytearray)
      values = _r(bytearray)
      return Dict.new keys,values
    end

    @offset += 1

    if t == 98
      return Flip.new _r(bytearray)
    end

    n = _ri(bytearray) 
    val = Array.new
    n.times do 
      item = readType[t].call
      val.push(item)
    end
  
    val
  end
end

