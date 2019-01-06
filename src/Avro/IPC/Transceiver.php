<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 06/01/2019
 * Time: 15:06
 */

namespace Avro\IPC;

/**
 * Class Transceiver
 * Abstract class to handle communication (framed read & write) between client & server
 * @package Avro\IPC
 */
abstract class Transceiver {

  const BUFFER_SIZE = 8192;

  protected $remote = null;

  /**
   * Processes a single request-reply interaction. Synchronous request-reply interaction.
   * @param string $request the request message
   * @return string the reply message
   */
  public function transceive($request) {
    $this->writeMessage($request);
    return $this->readMessage();
  }

  public function getRemote() {
    return $this->remote;
  }

  public function setRemote($remote) {
    $this->remote = $remote;
    return $this;
  }

  /**
   * @return string The name of the transceiver
   */
  abstract public function remoteName();

  /**
   * Reads a single message from the channel.
   * Blocks until a message can be read.
   * @return string The message read from the channel.
   */
  abstract public function readMessage();

  /**
   * Writes a message into the channel. Blocks until the message has been written.
   * @param string $message
   */
  abstract public function writeMessage($message);

  /**
   * Close this transceiver
   */
  abstract public function close();

  /**
   * Check if this transceiver has proceed to a valid handshake exchange
   * @return boolean true if this transceiver has make a valid hanshake with it's remote
   */
  abstract public function isConnected();
}
