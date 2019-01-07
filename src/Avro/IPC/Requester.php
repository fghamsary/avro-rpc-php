<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 06/01/2019
 * Time: 15:03
 */

namespace Avro\IPC;

use Avro\AvroUtil;
use Avro\Exception\AvroException;
use Avro\Exception\AvroRemoteException;
use Avro\Exception\AvroSchemaParseException;
use Avro\IO\AvroIOTypeException;
use Avro\IO\AvroStringIO;
use Avro\IO\Binary\AvroIOBinaryDecoder;
use Avro\IO\Binary\AvroIOBinaryEncoder;
use Avro\IO\Data\AvroDataIO;
use Avro\IO\Exception\AvroIOException;
use Avro\Protocol\AvroProtocol;
use Avro\Protocol\AvroProtocolParseException;
use Avro\Record\AvroErrorRecord;
use Avro\Record\AvroRecordHelper;
use Avro\Schema\AvroSchema;

/**
 * Class Requester
 *
 * Base class for the client side of a protocol interaction.
 *
 * @package Avro\IPC
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
  protected $localProtocol;

  /**
   * @var AvroProtocol[] the list of remote protocols indexed by the name of remote connections
   */
  protected $remoteProtocol = [];

  /**
   * @var string[] the list of remote protocol md5 hashes indexed by the name of remote connections
   */
  protected $remoteHash = [];

  /**
   * Remote avro protocol which should be used
   * @var AvroProtocol
   */
  protected $remote = null;

  /**
   * True if the Requester need to send it's protocol to the remote server
   * @var boolean
   */
  protected $sendProtocol = false;

  protected $handshakeRequesterWriterSchema;
  protected $handshakeRequesterReaderSchema;
  protected $metadataSchema;
  protected $meta_reader;

  protected $namespace;

  /**
   * Initializes a new requester object
   *
   * @param AvroProtocol $localProtocol Avro Protocol describing the messages sent and received.
   * @param Transceiver $transceiver Transceiver instance to channel messages through.
   *
   * @throws AvroSchemaParseException
   */
  public function __construct(AvroProtocol $localProtocol, Transceiver $transceiver) {
    $this->localProtocol = $localProtocol;
    $namespaceTokens = explode(".", $localProtocol->getNamespace());
    array_walk($namespaceTokens, function(&$token) { $token = ucfirst($token); });
    $this->namespace =  implode("\\", $namespaceTokens) . "\\";
    $this->transceiver = $transceiver;
    $this->handshakeRequesterWriterSchema = AvroSchema::parse(AvroDataIO::HANDSHAKE_REQUEST_SCHEMA_JSON);
    $this->handshakeRequesterReaderSchema = AvroSchema::parse(AvroDataIO::HANDSHAKE_RESPONSE_SCHEMA_JSON);
    $this->metadataSchema = AvroSchema::parse(AvroDataIO::METADATA_SCHEMA_JSON);
  }

  public function getLocalProtocol() {
    return $this->localProtocol;
  }

  public function getTransceiver() {
    return $this->transceiver;
  }

  /**
   * Writes a request message and reads a response or error message.
   *
   * @param string $messageName : name of the IPC method
   * @param mixed $requestDatum : IPC request
   *
   * @throws AvroException when $message_name is not registered on the local or remote protocol
   * @throws AvroRemoteException when server send an error
   *
   * @return mixed the result of the call based on the definition on the schema
   */
  public function request($messageName, $requestDatum) {
    $io = new AvroStringIO();
    $encoder = new AvroIOBinaryEncoder($io);
    $this->writeHandshakeRequest($encoder);
    $this->writeCallRequest($messageName, $requestDatum, $encoder);

    $callRequest = $io->string();
    if ($this->localProtocol->getRequestMessageByName($messageName)->isOneWay()) {
      $this->transceiver->writeMessage($callRequest);
      if (!$this->transceiver->isConnected()) {
        $handshakeResponse = $this->transceiver->readMessage();
        $io = new AvroStringIO($handshakeResponse);
        $decoder = new AvroIOBinaryDecoder($io);
        $this->readHandshakeResponse($decoder);
      }
      return true;
    } else {
      $callResponse = $this->transceiver->transceive($callRequest);
      // process the handshake and call response
      $io = new AvroStringIO($callResponse);
      $decoder = new AvroIOBinaryDecoder($io);
      $callResponseExists = $this->readHandshakeResponse($decoder);
      if ($callResponseExists) {
        $callResponse = $this->readCallResponse($messageName, $decoder);
        return $callResponse;
      } else {
        return $this->request($messageName, $requestDatum);
      }
    }
  }

  /**
   * Write the handshake request.
   *
   * @param AvroIOBinaryEncoder $encoder : Encoder to write the handshake request into.
   *
   * @throws AvroIOTypeException
   * @throws AvroIOException
   * @throws AvroProtocolParseException
   */
  public function writeHandshakeRequest(AvroIOBinaryEncoder $encoder) {
    if ($this->transceiver->isConnected()) {
      return;
    }
    $remoteName = $this->transceiver->remoteName();
    $localHash = $this->localProtocol->md5();
    $remoteHash = AvroUtil::arrayValue($this->remoteHash, $remoteName);
    if ($remoteHash === null) {
      $remoteHash = $localHash;
      $this->remote = $this->localProtocol;
    } else {
      $this->remote = AvroUtil::arrayValue($this->remoteProtocol, $remoteName);
    }
    $requestDatum = [
      'clientHash' => $localHash,
      'serverHash' => $remoteHash,
      'meta' => null,
      'clientProtocol' => $this->sendProtocol ? $this->localProtocol->__toString() : null
    ];
    $this->handshakeRequesterWriterSchema->write($requestDatum, $encoder);
  }

  /**
   * The format of a call request is:
   * - request metadata, a map with values of type bytes
   * - the message name, an Avro string, followed by
   * - the message parameters. Parameters are serialized according to the message's request declaration.
   * @param string $messageName : name of the IPC method
   * @param mixed $requestDatum : IPC request
   * @param AvroIOBinaryEncoder $encoder : Encoder to write the handshake request into.
   * @throws AvroException when $message_name is not registered on the local protocol
   */
  public function writeCallRequest($messageName, $requestDatum, AvroIOBinaryEncoder $encoder) {
    $request_metadata = [];
    $this->metadataSchema->write($request_metadata, $encoder);
    $message = $this->localProtocol->getRequestMessageByName($messageName);
    if ($message === null) {
      throw new AvroException("Unknown message: $messageName");
    }
    $encoder->writeString($messageName);
    $message->getRequest()->write($requestDatum, $encoder);
  }

  /**
   * Reads and processes the handshake response message.
   * @param AvroIOBinaryDecoder $decoder : Decoder to read messages from.
   * @return boolean true if a response exists.
   * @throws AvroException when server respond an unknown handshake match
   */
  public function readHandshakeResponse(AvroIOBinaryDecoder $decoder) {
    // if the handshake has been successfully made previously,
    // no need to do it again
    if ($this->transceiver->isConnected()) {
      return true;
    }
    $handshakeResponse = $this->handshakeRequesterReaderSchema->read($decoder);
    $match = $handshakeResponse["match"];

    switch ($match) {
      case 'BOTH':
        $established = true;
        $this->sendProtocol = false;
        break;
      case 'CLIENT':
        $established = true;
        $this->sendProtocol = false;
        $this->setRemote($handshakeResponse);
        break;
      case 'NONE':
        $this->sendProtocol = true;
        $this->setRemote($handshakeResponse);
        $established = false;
        break;
      default:
        throw new AvroException("Bad handshake response match: $match");
    }

    if ($established) {
      $this->transceiver->setRemote($this->remote);
    }

    return $established;
  }

  /**
   * @param array $handshakeResponse
   * @throws AvroProtocolParseException
   * @throws AvroSchemaParseException
   */
  protected function setRemote(array $handshakeResponse) {
    $this->remoteProtocol[$this->transceiver->remoteName()] = AvroProtocol::parse($handshakeResponse["serverProtocol"]);
    if (!isset($this->remoteHash[$this->transceiver->remoteName()])) {
      $this->remoteHash[$this->transceiver->remoteName()] = $handshakeResponse["serverHash"];
    }
  }

  /**
   * Reads and processes a method call response.
   * The format of a call response is:
   * - response metadata, a map with values of type bytes
   * - a one-byte error flag boolean, followed by either:
   * - if the error flag is false, the message response, serialized per the message's response schema.
   * - if the error flag is true, the error, serialized per the message's error union schema.
   * @param string $messageName : name of the IPC method
   * @param AvroIOBinaryDecoder $decoder : Decoder to read messages from.
   * @return boolean true if a response exists.
   * @throws AvroException $message_name is not registered on the local or remote protocol
   * @throws AvroRemoteException when server send an error
   */
  public function readCallResponse($messageName, AvroIOBinaryDecoder $decoder) {
    // returns the response metadata but we don't need it
    $this->metadataSchema->read($decoder);

    $remoteMessageSchema = $this->remote->getRequestMessageByName($messageName);
    if ($remoteMessageSchema === null) {
      throw new AvroException("Unknown remote message: $messageName");
    }

    $localMessageSchema = $this->localProtocol->getRequestMessageByName($messageName);
    if ($localMessageSchema === null) {
      throw new AvroException("Unknown local message: $messageName");
    }

    // No error raised on the server
    AvroRecordHelper::setDefaultNamespace($this->namespace);
    if (!$decoder->readBoolean()) {
      return $remoteMessageSchema->getResponse()->read($decoder, $localMessageSchema->getResponse());
    } else {
      $error = $remoteMessageSchema->getErrors()->read($decoder, $localMessageSchema->getErrors());
      if ($error instanceof AvroErrorRecord) {
        throw $error;
      } else {
        throw new AvroRemoteException($error);
      }
    }
  }

}
