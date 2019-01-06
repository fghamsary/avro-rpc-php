<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 06/01/2019
 * Time: 15:07
 */

namespace Avro\IPC;

/**
 * Class SocketTransceiver
 *
 * Socket Transceiver implementation.
 * This class can be used by a client to communicate with a socket server
 *
 * @package Avro\IPC
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
  public function __construct() {

  }

  /**
   * Create a SocketTransceiver with a new socket connected to $host:$port
   * @param string $host
   * @param int $port
   * @return SocketTransceiver
   */
  public static function create($host, $port) {
    $transceiver = new SocketTransceiver();
    $transceiver->createSocket($host, $port);

    return $transceiver;
  }

  protected function createSocket($host, $port) {
    $this->socket = socket_create(AF_INET, SOCK_STREAM, 0);
    socket_connect($this->socket, $host, $port);
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
  public function readMessage() {
    $message = "";
    while (true) {
      socket_recv($this->socket, $buf, 4, MSG_WAITALL);
      if ($buf == null) {
        return $buf;
      }
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
  public function writeMessage($message) {
    $binary_length = strlen($message);

    $max_binary_frame_length = self::BUFFER_SIZE - 4;
    $socket_ended_length = 0;

    $frames = array();
    while ($socket_ended_length < $binary_length) {
      $not_socket_ended_length = $binary_length - $socket_ended_length;
      $binary_frame_length = ($not_socket_ended_length > $max_binary_frame_length) ? $max_binary_frame_length : $not_socket_ended_length;
      $frames[] = substr($message, $socket_ended_length, $binary_frame_length);
      $socket_ended_length += $binary_frame_length;
    }

    foreach ($frames as $frame) {
      $msg = pack("N", strlen($frame)) . $frame;
      socket_write($this->socket, $msg, strlen($msg));
    }
    $footer = pack("N", 0);
    //socket_send ($this->socket, $header, strlen($header) , 0);
    socket_write($this->socket, $footer, strlen($footer));
  }

  /**
   * Check if this transceiver has proceed to a valid handshake exchange
   * @return boolean true if this transceiver has make a valid hanshake with it's remote
   */
  public function isConnected() {
    return $this->remote !== null;
  }

  /**
   * Return the name of the socket remode side
   * @return string the remote name
   */
  public function remoteName() {
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
