<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 06/01/2019
 * Time: 15:05
 */

namespace Avro\IPC;

use Avro\AvroUtil;
use Avro\Exception\AvroException;
use Avro\Exception\AvroRemoteException;
use Avro\IO\AvroStringIO;
use Avro\IO\Binary\AvroIOBinaryDecoder;
use Avro\IO\Binary\AvroIOBinaryEncoder;
use Avro\IO\Data\AvroDataIO;
use Avro\Protocol\AvroProtocol;
use Avro\Protocol\AvroProtocolMessage;
use Avro\Protocol\AvroProtocolParseException;
use Avro\Record\AvroErrorRecord;
use Avro\Schema\AvroPrimitiveSchema;
use Avro\Schema\AvroSchema;

/**
 * Class Responder
 * Base class for the server side of a protocol interaction.
 * @package Avro\IPC
 */
abstract class Responder {

  protected $localProtocol;
  protected $localHash;
  protected $protocolCache = [];

  protected $handshakeResponderWriterSchema;
  protected $handshakeResponderReaderSchema;
  protected $metadataSchema;

  protected $systemErrorSchema;

  public function __construct(AvroProtocol $localProtocol) {
    $this->localProtocol = $localProtocol;
    $this->localHash = $localProtocol->md5();
    $this->setProtocolCache($this->localHash, $localProtocol);

    $this->handshakeResponderWriterSchema = AvroSchema::parse(AvroDataIO::HANDSHAKE_RESPONSE_SCHEMA_JSON);
    $this->handshakeResponderReaderSchema = AvroSchema::parse(AvroDataIO::HANDSHAKE_REQUEST_SCHEMA_JSON);
    $this->metadataSchema = AvroSchema::parse(AvroDataIO::METADATA_SCHEMA_JSON);

    $this->systemErrorSchema = new AvroPrimitiveSchema(AvroProtocolMessage::SYSTEM_ERROR_TYPE);
  }

  /**
   * @param string $hash hash of an Avro Protocol
   * @return AvroProtocol|null The protocol associated with $hash or null
   */
  public function getProtocolCache($hash) {
    return AvroUtil::arrayValue($this->protocolCache, $hash);
  }

  /**
   * @param string $hash hash of an Avro Protocol
   * @param AvroProtocol $protocol
   * @return Responder $this
   */
  public function setProtocolCache($hash, AvroProtocol $protocol) {
    $this->protocolCache[$hash] = $protocol;
    return $this;
  }

  public function getLocalProtocol() {
    return $this->localProtocol;
  }

  /**
   * Entry point to process one procedure call.
   * @param string $callRequest the serialized procedure call request
   * @param Transceiver $transceiver the transceiver used for the response
   * @return string|null the serialized procedure call response or null if it's a one-way message
   * @throws AvroException
   */
  public function respond($callRequest, Transceiver $transceiver) {
    $bufferReader = new AvroStringIO($callRequest);
    $decoder = new AvroIOBinaryDecoder($bufferReader);

    $bufferWriter = new AvroStringIO();
    $encoder = new AvroIOBinaryEncoder($bufferWriter);

    $error = null;
    $responseMetadata = [];
    try {
      $remoteProtocol = $this->processHandshake($decoder, $encoder, $transceiver);
      if ($remoteProtocol === null) {
        return $bufferWriter->string();
      }

      // returns the request metadata but we don't need it
      $this->metadataSchema->read($decoder);
      $remoteMessageName = $decoder->readString();
      $remoteMessage = $remoteProtocol->getRequestMessageByName($remoteMessageName);
      if ($remoteMessage === null) {
        throw new AvroException("Unknown remote message: $remoteMessageName");
      }

      $localMessage = $this->localProtocol->getRequestMessageByName($remoteMessageName);
      if ($localMessage === null) {
        throw new AvroException("Unknown local message: $remoteMessageName");
      }

      $request = $remoteMessage->getRequest()->read($decoder, $localMessage->getRequest());
      $responseDatum = null;
      try {
        $responseDatum = $this->invoke($localMessage, $request);
        // if it's a one way message we only send the handshake if needed
        if ($localMessage->isOneWay()) {
          return ($bufferWriter->string() == "") ? null : $bufferWriter->string();
        }
      } catch (AvroRemoteException $e) {
        $error = $e;
      } catch (\Exception $e) {
        $error = new AvroRemoteException($e->getMessage());
      }

      $this->metadataSchema->write($responseMetadata, $encoder);
      $encoder->writeBoolean($error !== null);

      if ($error === null) {
        $localMessage->getResponse()->write($responseDatum, $encoder);
      } else {
        $errorsWriter = $remoteMessage->getErrors();
        if ($error instanceof AvroErrorRecord) {
          $errorsWriter->write($error, $encoder);
        } else {
          $errorsWriter->write($error->getMessage(), $encoder);
        }
      }

    } catch (AvroException $e) {
      $error = new AvroRemoteException($e->getMessage());
      $bufferWriter = new AvroStringIO();
      $encoder = new AvroIOBinaryEncoder($bufferWriter);
      $this->metadataSchema->write($responseMetadata, $encoder);
      $encoder->write_boolean($error !== null);
      $this->systemErrorSchema->write($error->getMessage(), $encoder);
    }

    return $bufferWriter->string();
  }

  /**
   * Processes an RPC handshake.
   *
   * @param AvroIOBinaryDecoder $decoder Where to read from
   * @param AvroIOBinaryEncoder $encoder Where to write to.
   * @param \Avro\IPC\Transceiver $transceiver the transceiver used for the response
   *
   * @return AvroProtocol The requested Protocol.

   * @throws AvroProtocolParseException
   */
  public function processHandshake(AvroIOBinaryDecoder $decoder, AvroIOBinaryEncoder $encoder, Transceiver $transceiver) {
    if ($transceiver->isConnected()) {
      return $transceiver->getRemote();
    }

    $handshakeRequest = $this->handshakeResponderReaderSchema->read($decoder);
    $clientHash = $handshakeRequest["clientHash"];
    $clientProtocol = $handshakeRequest["clientProtocol"];
    $remoteProtocol =  $this->getProtocolCache($clientHash);

    if ($remoteProtocol === null && $clientProtocol !== null) {
      $remoteProtocol = AvroProtocol::parse($clientProtocol);
      $this->setProtocolCache($clientHash, $remoteProtocol);
    }

    $serverHash = $handshakeRequest["serverHash"];
    $handshakeResponse = [];

    if ($this->localHash == $serverHash) {
      $handshakeResponse['match'] = $remoteProtocol === null ? 'NONE' : 'BOTH';
    } else {
      $handshakeResponse['match'] = $remoteProtocol === null ? 'NONE' : 'CLIENT';
    }

    $handshakeResponse["meta"] = null;
    if ($handshakeResponse['match'] != 'BOTH') {
      $handshakeResponse["serverProtocol"] = $this->localProtocol->__toString();
      $handshakeResponse["serverHash"] = $this->localHash;
    } else {
      $handshakeResponse["serverProtocol"] = null;
      $handshakeResponse["serverHash"] = null;
    }

    $this->handshakeResponderWriterSchema->write($handshakeResponse, $encoder);

    if ($handshakeResponse['match'] !== 'NONE') {
      $transceiver->setRemote($remoteProtocol);
    }

    return $remoteProtocol;
  }

  /**
   * Processes one procedure call
   * @param AvroProtocolMessage $localMessage
   * @param mixed $request Call request
   * @return mixed Call response
   * @throws AvroRemoteException
   */
  public abstract function invoke(AvroProtocolMessage $localMessage, $request);
}
