<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 05/01/2019
 * Time: 17:40
 */

namespace Avro\Protocol;

use Avro\AvroUtil;
use Avro\Exception\AvroException;
use Avro\Exception\AvroSchemaParseException;
use Avro\Record\AvroRecord;
use Avro\Schema\AvroEnumSchema;
use Avro\Schema\AvroNamedSchemata;
use Avro\Schema\AvroPrimitiveSchema;
use Avro\Schema\AvroRecordSchema;
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

  /**
   * @var string the md5String corresponding to the current protocol
   */
  private $md5String = null;

  /**
   * @param string $json The definition of the avro schema to be used
   *
   * @return AvroProtocol The protocol parsed based on the entry json
   *
   * @throws AvroProtocolParseException
   * @throws AvroSchemaParseException
   */
  public static function parse(string $json) {
    if ($json === null) {
      throw new AvroProtocolParseException("Protocol can't be null");
    }

    $protocol = new AvroProtocol();
    $protocol->realParse(json_decode($json, true));
    return $protocol;
  }

  /**
   * @param array $avro
   *
   * @throws AvroProtocolParseException
   * @throws AvroSchemaParseException
   */
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
   * @return string the name of the protocol used
   */
  public function getName() {
    return $this->name;
  }

  /**
   * @return string the namespace defined for this protocol
   */
  public function getNamespace() {
    return $this->namespace;
  }

  /**
   * @return AvroNamedSchemata the list of the schema records seen on the protocol
   */
  public function getSchemata() {
    return $this->schemata;
  }

  /**
   * @return AvroProtocolMessage[] the list of functions defined in the protocol indexed by their name
   */
  public function getMessages() {
    return $this->messages;
  }

  /**
   * @return string a md5 hash of this Avro Protocol
   *
   * @throws AvroProtocolParseException
   */
  public function getMd5() {
    return ($this->md5String !== null) ? pack("H*", $this->md5String) : md5($this->__toString(), true);
  }

  /**
   * Returns the currenty generated md5 saved on this avro protocol which may not be the current with the protocol
   *
   * @return string the md5 hash string of the current protocol
   */
  public function getMd5String() {
    return $this->md5String;
  }

  /**
   * This function generates md5String of the protocol and saves it to the local variable for later use
   *
   * @throws AvroProtocolParseException
   */
  public function generateMd5String() {
    $this->md5String = md5($this->__toString());
  }

  /**
   * @param AvroRecord $record the record which you want to serialize
   * @param bool $deepSerialization if true in case of having another AvroRecord as a field it will be serialized as well,
   *             and in case of false (default) only the first level is serialized and inner objects are return as empty object
   * @return array serialized object based on the schema
   * @throws AvroException if the $record is not defined in the current protocol definition
   */
  public function serializeObject(AvroRecord $record, $deepSerialization = false) {
    $fullName = $this->getNamespace() . '.' . $record::_getSimpleAvroClassName();
    if (!$this->getSchemata()->hasName($fullName)) {
      throw new AvroException('Record ' . $record::_getSimpleAvroClassName() . ' does not exist on this protocol!');
    }
    $schema = $this->getSchemata()->getSchema($fullName);
    $serializedObject = [];
    if ($schema instanceof AvroRecordSchema) {
      foreach ($schema->getFields() as $name => $field) {
        if ($field->getFieldType() instanceof AvroEnumSchema) {
          $serializedObject[$name] = (string)$record->_internalGetValue($name);
        } else {
          $serializedObject[$name] = $record->_internalGetValue($name);
          if ($deepSerialization && $serializedObject[$name] instanceof AvroRecordSchema) {
            $serializedObject[$name] = $this->serializeObject($serializedObject[$name], true);
          }
        }
      }
    }
    return $serializedObject;
  }

  /**
   * @return string the JSON-encoded representation of this Avro schema.
   *
   * @throws AvroProtocolParseException
   */
  public function __toString() {
    return json_encode($this->toAvro(), JSON_UNESCAPED_SLASHES);
  }

  /**
   * Internal representation of this Avro Protocol.
   * @return mixed
   *
   * @throws AvroProtocolParseException
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
