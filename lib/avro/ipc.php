<?php
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Avro;

use Avro\Record\AvroErrorRecord;
/**
 * Avro Requester/Responder and associated support classes.
 * @package Avro
 */

const HANDSHAKE_REQUEST_SCHEMA_JSON = <<<HRSJ
{
  "type": "record",
  "name": "HandshakeRequest", "namespace":"org.apache.avro.ipc",
  "fields": [
    {"name": "clientHash",
     "type": {"type": "fixed", "name": "MD5", "size": 16}},
    {"name": "clientProtocol", "type": ["null", "string"]},
    {"name": "serverHash", "type": "MD5"},
    {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}
  ]
}
HRSJ;

const HANDSHAKE_RESPONSE_SCHEMA_JSON = <<<HRSJ
{
  "type": "record",
  "name": "HandshakeResponse", "namespace": "org.apache.avro.ipc",
  "fields": [
    {"name": "match",
     "type": {"type": "enum", "name": "HandshakeMatch",
              "symbols": ["BOTH", "CLIENT", "NONE"]}},
    {"name": "serverProtocol",
     "type": ["null", "string"]},
    {"name": "serverHash",
     "type": ["null", {"type": "fixed", "name": "MD5", "size": 16}]},
    {"name": "meta",
     "type": ["null", {"type": "map", "values": "bytes"}]}
  ]
}
HRSJ;

const BUFFER_SIZE = 8192;


/**
 * Exceptions
 */

/**
 * Raised when an error message is sent by an Avro requester or responder.
 * @package Avro
 */
class AvroRemoteException extends AvroException {

}

/**
 * @package Avro
 */
class ConnectionClosedException extends AvroException { }



/**
 * Base IPC Classes (Requester/Responder)
 */

/**
 * Base class for the client side of a protocol interaction.
 * @package Avro
 */
class Requester {
  /**
   * The transceiver used to send the request
   * @var Transceiver
   */
  protected $transceiver;

  /**
   * The local avro protocol
   * @var AvroProtocol
   */
  protected $local_protocol;
  
  protected $remote_protocol = array();
  protected $remote_hash = array();
  /**
   * Remote avro protocol which should be used
   * @var AvroProtocol
   */
  protected $remote = null;
  /**
   * True if the Requester need to send it's protocol to the remote server
   * @var boolean
   */
  protected $send_protocol = false;
  
  protected $handshake_requester_writer;
  protected $handshake_requester_reader;
  protected $meta_writer;
  protected $meta_reader;

  protected $namespace;
  
  /**
   * Initializes a new requester object
   * @param AvroProtocol $local_protocol Avro Protocol describing the messages sent and received.
   * @param Transceiver $transceiver Transceiver instance to channel messages through.
   */
  public function __construct(AvroProtocol $local_protocol, Transceiver $transceiver) {
    $this->local_protocol = $local_protocol;
    $namespace_token = explode(".", $local_protocol->namespace);
    array_walk($namespace_token, function(&$token) { $token = ucfirst($token); });
    $this->namespace =  implode("\\", $namespace_token) . "\\";
    $this->transceiver = $transceiver;
    $this->handshake_requester_writer = new AvroIODatumWriter(AvroSchema::parse(HANDSHAKE_REQUEST_SCHEMA_JSON));
    $this->handshake_requester_reader = new AvroIODatumReader(AvroSchema::parse(HANDSHAKE_RESPONSE_SCHEMA_JSON));
    $this->meta_writer = new AvroIODatumWriter(AvroSchema::parse('{"type": "map", "values": "bytes"}'));
    $this->meta_reader = new AvroIODatumReader(AvroSchema::parse('{"type": "map", "values": "bytes"}'));
  }
  
  public function local_protocol() {
    return $this->local_protocol;
  }
  
  public function transceiver() {
    return $this->transceiver;
  }
  
  
  /**
   * Writes a request message and reads a response or error message.
   *
   * @param string $message_name : name of the IPC method
   * @param mixed $request_datum : IPC request
   *
   * @throws AvroException when $message_name is not registered on the local or remote protocol
   * @throws AvroRemoteException when server send an error
   *
   * @return mixed the result of the call based on the definition on the schema
   */
  public function request($message_name, $request_datum)
  {
    $io = new AvroStringIO();
    $encoder = new AvroIOBinaryEncoder($io);
    $this->write_handshake_request($encoder);
    $this->write_call_request($message_name, $request_datum, $encoder);
    
    $call_request = $io->string();
    if ($this->local_protocol->messages[$message_name]->is_one_way()) {
      $this->transceiver->write_message($call_request);
      if (!$this->transceiver->is_connected()) {
        $handshake_response = $this->transceiver->read_message();
        $io = new AvroStringIO($handshake_response);
        $decoder = new AvroIOBinaryDecoder($io);
        $this->read_handshake_response($decoder);
      }
      return true;
      
    } else {
      $call_response = $this->transceiver->transceive($call_request);
      // process the handshake and call response
      $io = new AvroStringIO($call_response);
      $decoder = new AvroIOBinaryDecoder($io);
      $call_response_exists = $this->read_handshake_response($decoder);
      if ($call_response_exists) {
        $call_response = $this->read_call_response($message_name, $decoder);
        return $call_response;
      } else {
        return $this->request($message_name, $request_datum);
      }
    }
  }
  
  /**
   * Write the handshake request.
   * @param AvroIOBinaryEncoder $encoder : Encoder to write the handshake request into.
   */
  public function write_handshake_request(AvroIOBinaryEncoder $encoder)
  {
    if ($this->transceiver->is_connected())
      return;
    
    $remote_name = $this->transceiver->remote_name();
    
    $local_hash = $this->local_protocol->md5();
    $remote_hash = (!isset($this->remote_hash[$remote_name])) ? null : $this->remote_hash[$remote_name];
    if ($remote_hash === null) {
      $remote_hash = $local_hash;
      $this->remote = $this->local_protocol;
    } else {
      $this->remote = $this->remote_protocol[$remote_name];
    }
    
    $request_datum = array('clientHash' => $local_hash, 'serverHash' => $remote_hash, 'meta' => null);
    $request_datum["clientProtocol"] = ($this->send_protocol) ? $this->local_protocol->__toString() : null;
    
    $this->handshake_requester_writer->write($request_datum, $encoder);
  }
  
  /**
   * The format of a call request is:
   * - request metadata, a map with values of type bytes
   * - the message name, an Avro string, followed by
   * - the message parameters. Parameters are serialized according to the message's request declaration.
   * @param string $message_name : name of the IPC method
   * @param mixed $request_datum : IPC request
   * @param AvroIOBinaryEncoder $encoder : Encoder to write the handshake request into.
   * @throws AvroException when $message_name is not registered on the local protocol
   */
  public function write_call_request($message_name, $request_datum, AvroIOBinaryEncoder $encoder)
  {
    $request_metadata = array();
    $this->meta_writer->write($request_metadata, $encoder);
    
    if (!isset($this->local_protocol->messages[$message_name]))
      throw new AvroException("Unknown message: $message_name");
    
    $encoder->write_string($message_name);
    $writer = new AvroIODatumWriter($this->local_protocol->messages[$message_name]->request);
    $writer->write($request_datum, $encoder);
  }
  
  /**
   * Reads and processes the handshake response message.
   * @param AvroIOBinaryDecoder $decoder : Decoder to read messages from.
   * @return boolean true if a response exists.
   * @throws AvroException when server respond an unknown handshake match
   */
  public function read_handshake_response(AvroIOBinaryDecoder $decoder)
  {
    // if the handshake has been successfully made previously,
    // no need to do it again
    if ($this->transceiver->is_connected())
      return true;
    
    $handshake_response = $this->handshake_requester_reader->read($decoder);
    $match = $handshake_response["match"];
    
    switch ($match) {
      case 'BOTH':
        $established = true;
        $this->send_protocol = false;
        break;
      case 'CLIENT':
        $established = true;
        $this->send_protocol = false;
        $this->set_remote($handshake_response);
        break;
      case 'NONE':
        $this->send_protocol = true;
        $this->set_remote($handshake_response);
        $established = false;
        break;
      default:
        throw new AvroException("Bad handshake response match: $match");
    }
    
    if ($established) {
      $this->transceiver->set_remote($this->remote);
    }
    
    return $established;
  }

  /**
   * @param $handshake_response
   * @throws AvroProtocolParseException
   */
  protected function set_remote($handshake_response) {
    $this->remote_protocol[$this->transceiver->remote_name()] = AvroProtocol::parse($handshake_response["serverProtocol"]);
    if (!isset($this->remote_hash[$this->transceiver->remote_name()]))
      $this->remote_hash[$this->transceiver->remote_name()] = $handshake_response["serverHash"];
  }
  
  /**
   * Reads and processes a method call response.
   * The format of a call response is:
   * - response metadata, a map with values of type bytes
   * - a one-byte error flag boolean, followed by either:
   * - if the error flag is false, the message response, serialized per the message's response schema.
   * - if the error flag is true, the error, serialized per the message's error union schema.
   * @param string $message_name : name of the IPC method
   * @param AvroIOBinaryDecoder $decoder : Decoder to read messages from.
   * @return boolean true if a response exists.
   * @throws AvroException $message_name is not registered on the local or remote protocol
   * @throws AvroRemoteException when server send an error
   */
  public function read_call_response($message_name, AvroIOBinaryDecoder $decoder) {
    // returns the response metadata but we don't need it
    $this->meta_reader->read($decoder);
    
    if (!isset($this->remote->messages[$message_name]))
      throw new AvroException("Unknown remote message: $message_name");
    $remote_message_schema = $this->remote->messages[$message_name];
    
    if (!isset($this->local_protocol->messages[$message_name]))
      throw new AvroException("Unknown local message: $message_name");
    $local_message_schema = $this->local_protocol->messages[$message_name];
    
    // No error raised on the server
    if (!$decoder->read_boolean()) {
      $datum_reader = new AvroIODatumReader($remote_message_schema->response, $local_message_schema->response);
      $datum_reader->set_default_namespace($this->namespace);
      return $datum_reader->read($decoder);
    } else {
      $datum_reader = new AvroIODatumReader($remote_message_schema->errors, $local_message_schema->errors);
      $datum_reader->set_default_namespace($this->namespace);
      $error = $datum_reader->read($decoder);
      if ($error instanceof AvroErrorRecord) {
        throw $error;
      } else {
        throw new AvroRemoteException($error);
      }
    }
  }
  
}

/**
 * Base class for the server side of a protocol interaction.
 */
abstract class Responder {
  
  protected $local_protocol;
  protected $local_hash;
  protected $protocol_cache = array();
  
  protected $handshake_responder_writer;
  protected $handshake_responder_reader;
  protected $meta_writer;
  protected $meta_reader;
  
  protected $system_error_schema;
  
  public function __construct(AvroProtocol $local_protocol) {
    $this->local_protocol = $local_protocol;
    $this->local_hash = $local_protocol->md5();
    $this->protocol_cache[$this->local_hash] = $this->local_protocol;
  
    $this->handshake_responder_writer = new AvroIODatumWriter(AvroSchema::parse(HANDSHAKE_RESPONSE_SCHEMA_JSON));
    $this->handshake_responder_reader = new AvroIODatumReader(AvroSchema::parse(HANDSHAKE_REQUEST_SCHEMA_JSON));
    $this->meta_writer = new AvroIODatumWriter(AvroSchema::parse('{"type": "map", "values": "bytes"}'));
    $this->meta_reader = new AvroIODatumReader(AvroSchema::parse('{"type": "map", "values": "bytes"}'));

    $this->system_error_schema = AvroSchema::parse('["string"]');
  }
  
  /**
   * @param string $hash hash of an Avro Protocol
   * @return AvroProtocol|null The protocol associated with $hash or null
   */
  public function get_protocol_cache($hash) {
    return (isset($this->protocol_cache[$hash])) ? $this->protocol_cache[$hash] : null;
  }
  
  /**
   * @param string $hash hash of an Avro Protocol
   * @param AvroProtocol $protocol
   * @return Responder $this
   */
  public function set_protocol_cache($hash, AvroProtocol $protocol) {
    $this->protocol_cache[$hash] = $protocol;
    return $this;
  }
  
  public function local_protocol() {
    return $this->local_protocol;
  }
  
  /**
   * Entry point to process one procedure call.
   * @param string $call_request the serialized procedure call request
   * @param Transceiver $transceiver the transceiver used for the response
   * @return string|null the serialiazed procedure call response or null if it's a one-way message
   * @throws AvroException
   */
  public function respond($call_request, Transceiver $transceiver) {
    $buffer_reader = new AvroStringIO($call_request);
    $decoder = new AvroIOBinaryDecoder($buffer_reader);
    
    $buffer_writer = new AvroStringIO();
    $encoder = new AvroIOBinaryEncoder($buffer_writer);
    
    $error = null;
    $response_metadata = array();
    try {
      $remote_protocol = $this->process_handshake($decoder, $encoder, $transceiver);
      if (is_null($remote_protocol)) 
          return $buffer_writer->string();

      // returns the request metadata but we don't need it
      $this->meta_reader->read($decoder);
      $remote_message_name = $decoder->read_string();
      if (!isset($remote_protocol->messages[$remote_message_name]))
        throw new AvroException("Unknown remote message: $remote_message_name");
      $remote_message = $remote_protocol->messages[$remote_message_name];
      
      if (!isset($this->local_protocol->messages[$remote_message_name]))
        throw new AvroException("Unknown local message: $remote_message_name");
      $local_message = $this->local_protocol->messages[$remote_message_name];
      
      $datum_reader = new AvroIODatumReader($remote_message->request, $local_message->request);
      $request = $datum_reader->read($decoder);
      $response_datum = null;
      try {
        $response_datum = $this->invoke($local_message, $request);
        // if it's a one way message we only send the handshake if needed
        if ($this->local_protocol->messages[$remote_message_name]->is_one_way()) {
          return ($buffer_writer->string() == "") ? null : $buffer_writer->string();
        }
      } catch (AvroRemoteException $e) {
        $error = $e;
      } catch (\Exception $e) {
        $error = new AvroRemoteException($e->getMessage());
      }
      
      $this->meta_writer->write($response_metadata, $encoder);
      $encoder->write_boolean(!is_null($error));
      
      if (is_null($error)) {
        $datum_writer = new AvroIODatumWriter($local_message->response);
        $datum_writer->write($response_datum, $encoder);
      } else {
        $datum_writer = new AvroIODatumWriter($remote_message->errors);
        if ($error instanceof AvroErrorRecord) {
          $datum_writer->write($error, $encoder);
        } else {
          $datum_writer->write($error->getMessage(), $encoder);
        }
      }
      
    } catch (AvroException $e) {
      $error = new AvroRemoteException($e->getMessage());
      $buffer_writer = new AvroStringIO();
      $encoder = new AvroIOBinaryEncoder($buffer_writer);
      $this->meta_writer->write($response_metadata, $encoder);
      $encoder->write_boolean(!is_null($error));
      $datum_writer = new AvroIODatumWriter($this->system_error_schema);
      $datum_writer->write($error->getMessage(), $encoder);
    }
    
    return $buffer_writer->string();
  }

  /**
   * Processes an RPC handshake.
   *
   * @param AvroIOBinaryDecoder $decoder Where to read from
   * @param AvroIOBinaryEncoder $encoder Where to write to.
   * @param Transceiver $transceiver the transceiver used for the response
   *
   * @return AvroProtocol The requested Protocol.

   * @throws AvroProtocolParseException
   */
  public function process_handshake(AvroIOBinaryDecoder $decoder, AvroIOBinaryEncoder $encoder, Transceiver $transceiver) {
    if ($transceiver->is_connected()) 
      return $transceiver->get_remote();
    
    $handshake_request = $this->handshake_responder_reader->read($decoder);
    $client_hash = $handshake_request["clientHash"];
    $client_protocol = $handshake_request["clientProtocol"];
    $remote_protocol =  $this->get_protocol_cache($client_hash);
    
    if (is_null($remote_protocol) && !is_null($client_protocol)) {
      $remote_protocol = AvroProtocol::parse($client_protocol);
      $this->set_protocol_cache($client_hash, $remote_protocol);
    }
    
    $server_hash = $handshake_request["serverHash"];
    $handshake_response = array();
    
    if ($this->local_hash == $server_hash)
      $handshake_response['match'] = (is_null($remote_protocol)) ? 'NONE' : 'BOTH';
    else
      $handshake_response['match'] = (is_null($remote_protocol)) ? 'NONE' : 'CLIENT';

    $handshake_response["meta"] = null;
    if ($handshake_response['match'] != 'BOTH') {
      $handshake_response["serverProtocol"] = $this->local_protocol->__toString();
      $handshake_response["serverHash"] = $this->local_hash;
    } else {
      $handshake_response["serverProtocol"] = null;
      $handshake_response["serverHash"] = null;
    }
    
    $this->handshake_responder_writer->write($handshake_response, $encoder);
    
    if ($handshake_response['match'] != 'NONE')
      $transceiver->set_remote($remote_protocol);

    return $remote_protocol;
  }
  
  /**
   * Processes one procedure call
   * @param AvroProtocolMessage $local_message
   * @param mixed $request Call request
   * @return mixed Call response
   * @throws AvroRemoteException
   */
  public abstract function invoke( $local_message, $request);
}

/**
 * Abstract class to handle communication (framed read & write) between client & server
 */
abstract class Transceiver {
  protected $remote = null;
  
  /**
   * Processes a single request-reply interaction. Synchronous request-reply interaction.
   * @param string $request the request message
   * @return string the reply message
   */
  public function transceive($request) {
    $this->write_message($request);
    return $this->read_message();
  }
  
  public function get_remote() {
    return $this->remote;
  }
  
  public function set_remote($remote) {
    $this->remote = $remote;
    return $this;
  }

  /**
   * @return string The name of the transceiver
   */
  abstract public function remote_name();
  
  /**
   * Reads a single message from the channel.
   * Blocks until a message can be read.
   * @return string The message read from the channel.
   */
  abstract public function read_message();
  
  /**
   * Writes a message into the channel. Blocks until the message has been written.
   * @param string $message
   */
  abstract public function write_message($message);
  
  /**
   * Close this transceiver
   */
  abstract public function close();
  
  /**
   * Check if this transceiver has proceed to a valid handshake exchange
   * @return boolean true if this transceiver has make a valid hanshake with it's remote
   */
  abstract public function is_connected();
}

/**
 * Socket Transceiver implementation.
 * This class can be used by a client to communicate with a socket server
 */
class SocketTransceiver extends Transceiver {
  
  protected $socket;
  
  /**
   * Construct a SocketTransceiver.
   * Do not use directly,
   * instead use the static function create & accept
   * depending on your context :
   *   - create for client transceiver &
   *   - accept for server transceiver
   */
  public function __construct() { }
  
  
  /**
   * Create a SocketTransceiver with a new socket connected to $host:$port
   * @param string $host
   * @param int $port
   * @return SocketTransceiver
   */
  public static function create($host, $port)  {
    $transceiver = new SocketTransceiver();
    $transceiver->createSocket($host, $port);
    
    return $transceiver;
  }
  
  protected function createSocket($host, $port) {
    $this->socket = socket_create(AF_INET, SOCK_STREAM, 0);
    socket_connect($this->socket , $host , $port);
  }
  
  /**
   * Create a SocketTransceiver based on the existing $socket
   * @param resource $socket
   * @return SocketTransceiver
   */
  public static function accept($socket) {
    $transceiver = new SocketTransceiver();
    $transceiver->acceptSocket($socket);
    
    return $transceiver;
  }
  
  protected function acceptSocket($socket) {
    $this->socket = socket_accept($socket);
  }
  
  /**
   * Reads a single message from the channel.
   * Blocks until a message can be read.
   * @return string The message read from the channel.
   */
  public function read_message() {
    $message = "";
    while (true) {
      socket_recv ($this->socket, $buf, 4, MSG_WAITALL);
      if ($buf == null)
        return $buf;
      $frame_size = unpack("Nsize", $buf);
      $frame_size = $frame_size["size"];
      if ($frame_size == 0) {
        return $message;
      }
      socket_recv($this->socket, $buf, $frame_size, MSG_WAITALL);
      $message .= $buf;
    }
    return $message;
  }
  
  /**
   * Writes a message into the channel. Blocks until the message has been written.
   * @param string $message
   */
  public function write_message($message) {
    $binary_length = strlen($message);
    
    $max_binary_frame_length = BUFFER_SIZE - 4;
    $socket_ended_length = 0;
    
    $frames = array();
    while ($socket_ended_length < $binary_length) {
      $not_socket_ended_length = $binary_length - $socket_ended_length;
      $binary_frame_length = ($not_socket_ended_length > $max_binary_frame_length) ? $max_binary_frame_length : $not_socket_ended_length;
      $frames[] = substr($message, $socket_ended_length, $binary_frame_length);
      $socket_ended_length += $binary_frame_length;
    }
    
    foreach ($frames as $frame) {
      $msg = pack("N", strlen($frame)).$frame;
      socket_write ( $this->socket, $msg, strlen($msg));
    }
    $footer = pack("N", 0);
    //socket_send ($this->socket, $header, strlen($header) , 0);
    socket_write ($this->socket, $footer, strlen($footer));
  }

  /**
   * Check if this transceiver has proceed to a valid handshake exchange
   * @return boolean true if this transceiver has make a valid hanshake with it's remote
   */
  public function is_connected() {
    return $this->remote !== null;
  }

  /**
   * Return the name of the socket remode side
   * @return string the remote name
   */
  public function remote_name() {
    $result = socket_getpeername($this->socket, $address, $port);
    return ($result) ? "$address:$port" : null;
  }

  public function close() {
    socket_close($this->socket);
  }

  public function socket() {
    return $this->socket;
  }

}

/**
 * Socket Transceiver implementation.
 * This class can be used by a client to communicate with a socket server (netty implementation)
 */
class NettyFramedSocketTransceiver extends SocketTransceiver {
  
  protected static $serial = 1;
  
  /**
   * Create a SocketTransceiver with a new socket connected to $host:$port
   * @param string $host
   * @param int $port
   * @return NettyFramedSocketTransceiver
   */
  public static function create($host, $port) {
    $transceiver = new NettyFramedSocketTransceiver();
    $transceiver->createSocket($host, $port);
    return $transceiver;
  }
  
  /**
   * Create a SocketTransceiver based on the existing $socket
   * @param resource $socket
   * @return NettyFramedSocketTransceiver
   */
  public static function accept($socket) {
    $transceiver = new NettyFramedSocketTransceiver();
    $transceiver->acceptSocket($socket);
    return $transceiver;
  }
  
  /**
   * Reads a single message from the channel.
   * Blocks until a message can be read.
   * @return string The message read from the channel.
   */
  public function read_message() {
    socket_recv ( $this->socket , $buf , 8 , MSG_WAITALL );
    if ($buf == null)
      return $buf;
    
    $frame_count = unpack("Nserial/Ncount", $buf);
    $frame_count = $frame_count["count"];
    $message = "";
    for ($i = 0; $i < $frame_count; $i++ ) {
      socket_recv($this->socket, $buf, 4, MSG_WAITALL);
      $frame_size = unpack("Nsize", $buf);
      $frame_size = $frame_size["size"];
      if ($frame_size > 0) {
        socket_recv($this->socket, $bif, $frame_size, MSG_WAITALL);
        $message .= $bif;
      }
    }
    return $message;
  }

  /**
   * Writes a message into the channel. Blocks until the message has been written.
   * @param string $message
   */
  public function write_message($message) {
    $binary_length = strlen($message);
    
    $max_binary_frame_length = BUFFER_SIZE - 4;
    $socket_ended_length = 0;
    
    $frames = array();
    while ($socket_ended_length < $binary_length) {
      $not_socket_ended_length = $binary_length - $socket_ended_length;
      $binary_frame_length = ($not_socket_ended_length > $max_binary_frame_length) ? $max_binary_frame_length : $not_socket_ended_length;
      $frames[] = substr($message, $socket_ended_length, $binary_frame_length);
      $socket_ended_length += $binary_frame_length;
    }
    
    $header = pack("N", self::$serial++).pack("N", count($frames));
    //socket_send ($this->socket, $header, strlen($header) , 0);
    socket_write($this->socket, $header, strlen($header));
    foreach ($frames as $frame) {
      $msg = pack("N", strlen($frame)).$frame;
      socket_write($this->socket, $msg, strlen($msg));
    }
  }
}


/**
 * Socket server implementation.
 */
class SocketServer {

  /**
   * @var Responder
   */
  protected $responder;
  protected $socket;
  protected $use_netty_framed_transceiver;
  
  public function __construct($host, $port, Responder $responder, $use_netty_framed_transceiver = false) {
    $this->responder = $responder;
    $this->socket = socket_create(AF_INET, SOCK_STREAM, 0);
    $this->use_netty_framed_transceiver = $use_netty_framed_transceiver;
    socket_bind($this->socket , $host , $port);
    socket_listen($this->socket, 3);
  }
  
  public function start($max_clients = 10) {
    $transceivers = array();
    
    while (true) {
      // $read contains all the client we listen to
      $read = array($this->socket);
      for ($i = 0; $i < $max_clients; $i++) {
        if (isset($transceivers[$i]) && $transceivers[$i]  != null)
          $read[$i + 1] = $transceivers[$i]->socket();
      }
    
      // check all client to know which ones are writing
      socket_select($read, $write, $except, null);
      // $read contains all client that send something to the server
      
      // New connexion
      if (in_array($this->socket, $read)) {
        for ($i = 0; $i < $max_clients; $i++) {
          if ( !isset($transceivers[$i]) ) {
            $transceivers[$i] = ($this->use_netty_framed_transceiver)
                                  ? NettyFramedSocketTransceiver::accept($this->socket)
                                  : SocketTransceiver::accept($this->socket);
            break;
          }
        }
      }
      
      // Check all client that are trying to write
      for ($i = 0; $i < $max_clients; $i++) {
        if (isset($transceivers[$i]) && in_array($transceivers[$i]->socket(), $read)) {
          try {
            $is_closed = $this->handle_request($transceivers[$i]);
            if ($is_closed) {
              unset($transceivers[$i]);
            }
          } catch (AvroException $e) {
            error_log($e->getMessage());
            unset($transceivers[$i]);
          }
        }
      }
    }
    
    socket_close($this->socket);
  }

  /**
   * @param Transceiver $transceiver
   * @return bool
   * @throws AvroException
   */
  public function handle_request(Transceiver $transceiver) {
    // Read the message
    $call_request = $transceiver->read_message();
    
    // Respond if the message is not empty
    if ($call_request !== null) {
      $call_response = $this->responder->respond($call_request, $transceiver);
      if ($call_response !== null) {
        $transceiver->write_message($call_response);
      }
      return false;
    // Else the client has disconnect
    } else {
      $transceiver->close();
      return true;
    }
  }
}
