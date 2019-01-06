<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 06/01/2019
 * Time: 15:09
 */

namespace Avro\IPC;

use Avro\Exception\AvroException;

/**
 * Class SocketServer
 *
 * Socket server implementation.
 *
 * @package Avro\IPC
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
    socket_bind($this->socket, $host, $port);
    socket_listen($this->socket, 3);
  }

  public function start($max_clients = 10) {
    $transceivers = array();

    while (true) {
      // $read contains all the client we listen to
      $read = array($this->socket);
      for ($i = 0; $i < $max_clients; $i++) {
        if (isset($transceivers[$i]) && $transceivers[$i] != null) {
          $read[$i + 1] = $transceivers[$i]->socket();
        }
      }

      // check all client to know which ones are writing
      socket_select($read, $write, $except, null);
      // $read contains all client that send something to the server

      // New connexion
      if (in_array($this->socket, $read)) {
        for ($i = 0; $i < $max_clients; $i++) {
          if (!isset($transceivers[$i])) {
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
    $call_request = $transceiver->readMessage();

    // Respond if the message is not empty
    if ($call_request !== null) {
      $call_response = $this->responder->respond($call_request, $transceiver);
      if ($call_response !== null) {
        $transceiver->writeMessage($call_response);
      }
      return false;
      // Else the client has disconnect
    } else {
      $transceiver->close();
      return true;
    }
  }
}
