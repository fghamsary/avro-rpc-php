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
use Avro\Record\AvroRecordHelper;
use Avro\Record\IAvroRecordBase;
use Avro\Schema\AvroEnumSchema;
use Avro\Schema\AvroMapSchema;
use Avro\Schema\AvroNamedSchemata;
use Avro\Schema\AvroPrimitiveSchema;
use Avro\Schema\AvroRecordSchema;
use Avro\Schema\AvroSchema;
use Avro\Schema\AvroUnionSchema;
use Countable;

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
   * @param IAvroRecordBase $record the record which you want to serialize
   * @param bool $deepSerialization if true in case of having another AvroRecord as a field it will be serialized as well,
   *             and in case of false (default) only the first level is serialized and inner objects are return as empty object
   * @param bool $keepKeys if true we keep the keys on maps if not we use a simple array
   * @return array serialized object based on the schema
   * @throws AvroException if the $record is not defined in the current protocol definition
   */
  public function serializeObject(IAvroRecordBase $record, $deepSerialization = false, $keepKeys = true) {
    $fullName = $this->getNamespace() . '.' . $record::_getSimpleAvroClassName();
    if (!$this->getSchemata()->hasName($fullName)) {
      throw new AvroException('Record ' . $record::_getSimpleAvroClassName() . ' does not exist on this protocol!');
    }
    $schema = $this->getSchemata()->getSchema($fullName);
    $serializedObject = [];
    if ($schema instanceof AvroRecordSchema) {
      foreach ($schema->getFields() as $name => $field) {
        $fieldType = $field->getFieldType();
        if ($fieldType instanceof AvroEnumSchema) {
          $serializedObject[$name] = (string)$record->_internalGetValue($name);
        } else {
          $serializedObject[$name] = $record->_internalGetValue($name);
          if ($deepSerialization) {
            if ($serializedObject[$name] instanceof IAvroRecordBase) {
              $serializedObject[$name] = $this->serializeObject($serializedObject[$name], true, $keepKeys);
            } elseif ((is_array($serializedObject[$name]) || $serializedObject[$name] instanceof Countable)) {
              if (count($serializedObject[$name]) > 0) {
                $innerValue = $serializedObject[$name];
                $firstValue = null;
                if (is_array($innerValue)) {
                  $firstValue = array_values($innerValue)[0];
                } elseif ($innerValue instanceof \IteratorAggregate) {
                  $firstValue = $innerValue->getIterator()->current();
                } elseif ($innerValue instanceof \ArrayIterator){
                  $firstValue = $innerValue->current();
                } else {
                  $firstValue = null;
                  throw new AvroException("[AvroProtocol Serialization]: There is a problem with value for JSON serialization in $name for the object of type $fieldType in schema $fullName: " . json_encode($innerValue));
                }
                if ($firstValue instanceof IAvroRecordBase) {
                  $serializedObject[$name] = [];
                  /**
                   * @var string $key
                   * @var AvroRecord $value
                   */
                  foreach ($innerValue as $key => $value) {
                    $serializedValue = $this->serializeObject($value, true, $keepKeys);
                    if ($keepKeys) {
                      $serializedObject[$name][$key] = $serializedValue;
                    } else {
                      $serializedObject[$name][] = $serializedValue;
                    }
                  }
                }
              } else {
                $isMapBased = $fieldType instanceof AvroMapSchema ||
                  ($fieldType instanceof AvroUnionSchema && $fieldType->getNullableSchema() instanceof AvroMapSchema);
                $serializedObject[$name] = $isMapBased ? new \stdClass() : [];
              }
            }
          }
        }
      }
    }
    return $serializedObject;
  }

  /**
   * @param IAvroRecordBase $record the record which should be populated with the values in the array of serializedObject
   * @param array $serializedObject the serialized representation of the item as an associative array
   * @throws AvroException if the $record is not defined in the current protocol definition
   */
  public function deserializeObject(IAvroRecordBase $record, array $serializedObject) {
    $fullName = $this->getNamespace() . '.' . $record::_getSimpleAvroClassName();
    if (!$this->getSchemata()->hasName($fullName)) {
      throw new AvroException('Record ' . $record::_getSimpleAvroClassName() . ' does not exist on this protocol!');
    }
    $schema = $this->getSchemata()->getSchema($fullName);
    if ($schema instanceof AvroRecordSchema) {
      foreach ($schema->getFields() as $name => $field) {
        $fieldType = $field->getFieldType();
        if (array_key_exists($name, $serializedObject)) {
          $result = $fieldType->deserializeJson($serializedObject[$name]);
          $record->_internalSetValue($name, $result);
        }
      }
    }
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
