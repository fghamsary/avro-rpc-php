<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 06/01/2019
 * Time: 15:08
 */

namespace Avro\IPC;

/**
 * Class NettyFramedSocketTransceiver
 *
 * Socket Transceiver implementation.
 * This class can be used by a client to communicate with a socket server (netty implementation)
 *
 * @package Avro\IPC
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
  public function readMessage() {
    socket_recv($this->socket, $buf, 8, MSG_WAITALL);
    if ($buf == null) {
      return $buf;
    }

    $frame_count = unpack("Nserial/Ncount", $buf);
    $frame_count = $frame_count["count"];
    $message = "";
    for ($i = 0; $i < $frame_count; $i++) {
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

    $header = pack("N", self::$serial++) . pack("N", count($frames));
    //socket_send ($this->socket, $header, strlen($header) , 0);
    socket_write($this->socket, $header, strlen($header));
    foreach ($frames as $frame) {
      $msg = pack("N", strlen($frame)) . $frame;
      socket_write($this->socket, $msg, strlen($msg));
    }
  }
}
