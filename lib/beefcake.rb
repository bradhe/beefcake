require 'socket'
require 'beefcake/buffer'

module Beefcake
  module Message

    class WrongTypeError < StandardError
      def initialize(name, exp, got)
        super("Wrong type `#{got}` given for (#{name}).  Expected #{exp}")
      end
    end


    class InvalidValueError < StandardError
      def initialize(name, val)
        super("Invalid Value given for `#{name}`: #{val.inspect}")
      end
    end


    class RequiredFieldNotSetError < StandardError
      def initialize(name)
        super("Field #{name} is required but nil")
      end
    end

    class DuplicateFieldNumber < StandardError
      def initialize(num, name)
        super("Field number #{num} (#{name}) was already used")
      end
    end

    class Field < Struct.new(:rule, :name, :type, :fn, :opts)
      def <=>(o)
        fn <=> o.fn
      end
    end


    module Dsl
      def required(name, type, fn, opts={})
        field(:required, name, type, fn, opts)
      end

      def repeated(name, type, fn, opts={})
        field(:repeated, name, type, fn, opts)
      end

      def optional(name, type, fn, opts={})
        field(:optional, name, type, fn, opts)
      end

      def field(rule, name, type, fn, opts)
        if fields.include?(fn)
          raise DuplicateFieldNumber.new(fn, name)
        end
        fields[fn] = Field.new(rule, name, type, fn, opts)
        attr_accessor name
      end

      def fields
        @fields ||= {}
      end
    end

    module Encode

      def encode(buf = Buffer.new)
        validate!

        if ! buf.respond_to?(:<<)
          raise ArgumentError, "buf doesn't respond to `<<`"
        end

        if ! buf.is_a?(Buffer)
          buf = Buffer.new(buf)
        end

        # TODO: Error if any required fields at nil

        __beefcake_fields__.values.sort.each do |fld|
          if fld.opts[:packed]
            bytes = encode!(Buffer.new, fld, 0)
            buf.append_info(fld.fn, Buffer.wire_for(fld.type))
            buf.append_uint64(bytes.length)
            buf << bytes
          else
            encode!(buf, fld, fld.fn)
          end
        end

        buf
      end

      def encode!(buf, fld, fn)
        v = self[fld.name]
        v = v.is_a?(Array) ? v : [v]

        v.compact.each do |val|
          case fld.type
          when Class # encodable
            # TODO: raise error if type != val.class
            buf.append(:string, val.encode, fn)
          when Module # enum
            if ! valid_enum?(fld.type, val)
              raise InvalidValueError.new(fld.name, val)
            end

            buf.append(:int32, val, fn)
          else
            buf.append(fld.type, val, fn)
          end
        end

        buf
      end

      def write_delimited(buf = Buffer.new)
        if ! buf.respond_to?(:<<)
          raise ArgumentError, "buf doesn't respond to `<<`"
        end

        if ! buf.is_a?(Buffer)
          buf = Buffer.new(buf)
        end

        buf.append_bytes(encode)

        buf
      end

      def valid_enum?(mod, val)
        !!name_for(mod, val)
      end

      def name_for(mod, val)
        mod.constants.each do |name|
          if mod.const_get(name) == val
            return name
          end
        end
        nil
      end

      def validate!
        __beefcake_fields__.values.each do |fld|
          if fld.rule == :required && self[fld.name].nil?
            raise RequiredFieldNotSetError, fld.name
          end
        end
      end

    end


    module Decode
      def decode(buf, o=self.new)
        if ! buf.is_a?(Buffer)
          buf = Buffer.new(buf)
        end

        # TODO: test for incomplete buffer
        while buf.length > 0
          fn, wire = buf.read_info

          fld = _fields[fn]

          # We don't have a field for with index fn.
          # Ignore this data and move on.
          if fld.nil?
            buf.skip(wire)
            next
          end

          exp = Buffer.wire_for(fld.type)
          if wire != exp
            raise WrongTypeError.new(fld.name, exp, wire)
          end

          if fld.rule == :repeated && fld.opts[:packed]
            len = buf.read_uint64
            tmp = Buffer.new(buf.read(len))
            o[fld.name] ||= []
            while tmp.length > 0
              o[fld.name] << tmp.read(fld.type)
            end
          elsif fld.rule == :repeated
            val = buf.read(fld.type)
            (o[fld.name] ||= []) << val
          else
            val = buf.read(fld.type)
            o[fld.name] = val
          end
        end

        # Set defaults
        _fields.values.each do |f|
          next if o[f.name] == false
          o[f.name] ||= f.opts[:default]
        end

        o.validate!

        o
      end

      def read_delimited(buf, o=self.new)
        if ! buf.is_a?(Buffer)
          buf = Buffer.new(buf)
        end

        return if buf.length == 0

        n = buf.read_int64
        tmp = Buffer.new(buf.read(n))

        decode(tmp, o)
      end
    end


    def self.included(o)
      o.extend Dsl
      o.extend Decode
      o.send(:include, Encode)
    end

    def initialize(attrs={})
      __beefcake_fields__.values.each do |fld|
        self[fld.name] = attrs[fld.name]
      end
    end

    def __beefcake_fields__
      self.class.fields
    end

    def [](k)
      __send__(k)
    end

    def []=(k, v)
      __send__("#{k}=", v)
    end

    def ==(o)
      return false if (o == nil) || (o == false)
      return false unless o.is_a? self.class
      __beefcake_fields__.values.all? {|fld| self[fld.name] == o[fld.name] }
    end

    def inspect
      set = __beefcake_fields__.values.select {|fld| self[fld.name] != nil }

      flds = set.map do |fld|
        val = self[fld.name]

        case fld.type
        when Class
          "#{fld.name}: #{val.inspect}"
        when Module
          title = name_for(fld.type, val) || "-NA-"
          "#{fld.name}: #{title}(#{val.inspect})"
        else
          "#{fld.name}: #{val.inspect}"
        end
      end

      "<#{self.class.name} #{flds.join(", ")}>"
    end

    def to_hash
      __beefcake_fields__.values.inject({}) do |h, fld|
        value = self[fld.name]
        unless value.nil?
          h[fld.name] = value
        end
        h
      end
    end

  end

  class RPCMessage
    include Beefcake::Message

    optional :service_method, :string, 1
    optional :seq,            :uint64, 2
    optional :error,          :string, 3
  end

  class Client
    attr_reader :host, :port

    def initialize(host, port)
      @host, @port = [host.to_s, port.to_i]
    end

    protected

    def send_request(service_method, obj, opts={})
      header = RPCMessage.new(service_method: service_method, seq: 0)
      write_all(header)
      write_all(obj)

      return_klass = opts[:returns] || opts['returns']
      read_all(return_klass)
    end

    private

    def write_all(message)
      str = encoded(message)

      buf = Buffer.new
      buf.append_uint64(str.length)
      buf << str

      socket.write(buf)
      socket.flush
    end

    def read_all(klass)
      # TODO: Anything with this message?
      read_obj(RPCMessage)
      read_obj(klass)
    end

    def read_obj(klass)
      msg = socket.read(read_obj_size)
      klass.decode(msg)
    end

    def read_obj_size
      buf = Buffer.new
      read_uvarint(buf)
      size = buf.read_uint64
    end

    def read_uvarint(buf)
      b = socket.recv(1).ord
      buf << b
      read_uvarint(buf) if (b >> 7) & 0x01 == 0x01
    end

    def encoded(obj)
      ''.tap { |s| obj.encode(s) }
    end

    def socket
      @socket ||= open_socket!
    end

    def open_socket!(timeout=5)
      addr = Socket.getaddrinfo(host, nil)
      sock = Socket.new(Socket.const_get(addr[0][0]), Socket::SOCK_STREAM, 0)

      # If a timeout was requested, let's configure one.
      if timeout
        secs = timeout.to_i
        usecs = Integer((timeout - secs) * 1_000_000)
        optval = [secs, usecs].pack("l_2")
        begin
          sock.setsockopt Socket::SOL_SOCKET, Socket::SO_RCVTIMEO, optval
          sock.setsockopt Socket::SOL_SOCKET, Socket::SO_SNDTIMEO, optval
        rescue Exception => ex
          warn "Unable to use raw socket timeouts: #{ex.class.name}: #{ex.message}"
        end
      end

      sock.connect(Socket.pack_sockaddr_in(port, addr[0][3]))
      sock
    end
  end
end
