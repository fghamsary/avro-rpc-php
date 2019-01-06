<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 05/01/2019
 * Time: 17:40
 */

namespace Avro\Protocol;

use Avro\AvroUtil;
use Avro\Schema\AvroNamedSchemata;
use Avro\Schema\AvroSchema;

/**
 * Class AvroProtocol is the library of Avro for protocol
 * @package Avro\Protocol
 */
class AvroProtocol {

  /**
   * @var string the name of the protocol
   */
  private $name;

  /**
   * @var string the namespace of the protocol
   */
  private $namespace;

  /**
   * @var string the documentation of the protocol
   */
  private $doc = null;

  /**
   * @var AvroNamedSchemata
   */
  private $schemata;

  /**
   * @var AvroProtocolMessage[] indexed by the name of the message
   */
  private $messages = [];

  private $md5string = null;

  /**
   * @param string $json The definition of the avro schema to be used
   * @return AvroProtocol The protocol parsed based on the entry json
   * @throws AvroProtocolParseException
   */
  public static function parse(string $json) {
    if ($json === null) {
      throw new AvroProtocolParseException("Protocol can't be null");
    }

    $protocol = new AvroProtocol();
    $protocol->realParse(json_decode($json, true));
    return $protocol;
  }

  private function realParse(array $avro) {
    $this->name = $avro["protocol"];
    $this->namespace = $avro["namespace"];
    $this->schemata = new AvroNamedSchemata();
    $this->doc = AvroUtil::arrayValue($avro,'doc');
    $types = AvroUtil::arrayValue($avro, 'types');
    if ($types !== null) {
      AvroSchema::realParse($types, $this->namespace, $this->schemata);
    }
    $messages = AvroUtil::arrayValue($avro, 'messages');
    if ($messages !== null && is_array($messages)) {
      foreach ($messages as $messageName => $messageAvro) {
        $this->messages[$messageName] = new AvroProtocolMessage($messageName, $messageAvro, $this);
      }
    }
  }

  /**
   * Returns the schema of fields as an array for the request based on the definition
   *
   * @param string $method the name of the method
   * @return array the schema of the fields based on the definition
   */
  public function requestSchemas(string $method) {
    $schemas = [];
    $messages = $this->messages;
    $msgs = $messages[$method];
    foreach ($msgs->getRequest()->getFields() as $field) {
      $schemas[] = $field->getFieldType();
    }
    return $schemas;
  }

  /**
   * @param string $method the name of the method that should be checked from the protocol
   * @return AvroProtocolMessage|null the message corresponding to the method name provided or null if not found
   */
  public function getRequestMessageByName(string $method) {
    return AvroUtil::arrayValue($this->messages, $method);
  }

  /**
   * @return string the namespace defined for this protocol
   */
  public function getNamespace() {
    return $this->namespace;
  }

  /**
   * @return AvroNamedSchemata the list of the schema recrods seen on the protocol
   */
  public function getSchemata() {
    return $this->schemata;
  }

  /**
   * @return string a md5 hash of this Avro Protocol
   */
  public function md5() {
    return ($this->md5string != null) ? pack("H*", $this->md5string) : md5($this->__toString(), true);
  }

  /**
   * @returns string the JSON-encoded representation of this Avro schema.
   */
  public function __toString() {
    return json_encode($this->toAvro(), JSON_UNESCAPED_SLASHES);
  }

  /**
   * Internal representation of this Avro Protocol.
   * @returns mixed
   */
  public function toAvro() {
    $avro = [
      "protocol" => $this->name,
      "namespace" => $this->namespace
    ];
    if ($this->doc !== null) {
      $avro["doc"] = $this->doc;
    }
    $avro["types"] = $this->getSchemata()->toAvro();
    $messages = array();
    foreach ($this->messages as $name => $msg) {
      $messages[$name] = $msg->toAvro();
    }
    $avro["messages"] = $messages;
    return $avro;
  }
}
