<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 05/01/2019
 * Time: 18:13
 */

namespace Avro\Protocol;


use Avro\AvroUtil;
use Avro\Exception\AvroSchemaParseException;
use Avro\Schema\AvroName;
use Avro\Schema\AvroPrimitiveSchema;
use Avro\Schema\AvroRecordSchema;
use Avro\Schema\AvroSchema;
use Avro\Schema\AvroUnionSchema;

/**
 * Class AvroProtocolMessage is a representation of the message defined in an avro protocol
 * @package Avro\Protocol
 */
class AvroProtocolMessage {

  const SYSTEM_ERROR_TYPE = "string";

  /**
   * @var string the documentation for this method
   */
  private $doc = null;

  /**
   * @var AvroName the name of the method and default namespace
   */
  private $name;

  /**
   * @var AvroRecordSchema $request the request as an avro schema
   */
  private $request;

  /**
   * @var AvroSchema|null
   */
  private $response = null;

  /**
   * @var AvroUnionSchema
   */
  private $errors = null;

  /**
   * @var boolean true if the method is one way an have no result
   */
  private $isOneWay = false;

  /**
   * AvroProtocolMessage constructor.
   * @param string $name the name of the message on the protocol
   * @param array $avro the definition which will be parsed
   * @param AvroProtocol $protocol
   * @throws AvroSchemaParseException
   * @throws AvroProtocolParseException
   */
  public function __construct(string $name, array $avro, AvroProtocol $protocol) {
    $namespace = $protocol->getNamespace();
    $this->name = new AvroName($name, null, $protocol->getNamespace());
    $this->doc = AvroUtil::arrayValue($avro, 'doc');
    $this->request = new AvroRecordSchema($this->name,
      null,
      $avro['request'],
      $protocol->getSchemata(),
      AvroSchema::REQUEST_SCHEMA
    );
    $response = AvroUtil::arrayValue($avro, 'response');
    if ($response !== null) {
      if (!is_array($response)) {
        $this->response = $protocol->getSchemata()->schemaByName(new AvroName($response, null, $namespace));
        if ($this->response === null) {
          if (AvroSchema::isPrimitiveType($response)) {
            // this is a primitive type so we create new primitive type directly
            $this->response = new AvroPrimitiveSchema($response);
          } else {
            throw new AvroSchemaParseException("Response $response is not known for $name message!");
          }
        }
      } else {
        $this->response = AvroSchema::realParse($response, $namespace, $protocol->getSchemata());
      }
    } else {
      $this->response = new AvroPrimitiveSchema(AvroSchema::NULL_TYPE);
    }

    $this->isOneWay = AvroUtil::arrayValue($avro, 'one-way') === true;
    if ($this->isOneWay && $this->getResponse() !== null && $this->getResponse()->getType() !== AvroSchema::NULL_TYPE) {
      throw new AvroProtocolParseException("One way message $name can't have a response");
    }

    $errorDefinitions = AvroUtil::arrayValue($avro, 'errors');
    if ($this->isOneWay && $errorDefinitions !== null) {
      throw new AvroProtocolParseException("One way message $name can't have errors");
    }

    if (!$this->isOneWay) {
      $errors = [
        self::SYSTEM_ERROR_TYPE
      ];
      if ($errorDefinitions !== null) {
        if (!is_array($errorDefinitions)) {
          throw new AvroProtocolParseException("Errors must be an array");
        }
        foreach ($errorDefinitions as $errorType) {
          $errorSchema = $protocol->getSchemata()->schemaByName(new AvroName($errorType, null, $namespace));
          if ($errorSchema === null) {
            throw new AvroProtocolParseException("Error type $errorType is unknown");
          }
          if ($errorSchema instanceof AvroRecordSchema && $errorSchema->getType() === AvroSchema::ERROR_SCHEMA) {
            $errors[] = $errorSchema->getQualifiedName();
          } else {
            throw new AvroProtocolParseException("Error type $errorType is not defined as error in the schema!");
          }
        }
      }
      $this->errors = new AvroUnionSchema($errors, $namespace, $protocol->getSchemata());
    }
  }

  public function isOneWay() {
    return $this->isOneWay;
  }

  /**
   * @return string the name of message in the protocol
   */
  public function getName() {
    return $this->name->getName();
  }

  /**
   * @return AvroRecordSchema the request definition of the method
   */
  public function getRequest() {
    return $this->request;
  }

  /**
   * @return AvroSchema|null the response definition of the method
   */
  public function getResponse() {
    return $this->response;
  }

  /**
   * @return string|null the document for this method defined in the schema
   */
  public function getDocumentation() {
    return $this->doc;
  }

  /**
   * @return AvroUnionSchema|null the errors definition of the method
   */
  public function getErrors() {
    return $this->errors;
  }

  /**
   * @return array
   * @throws AvroProtocolParseException
   */
  public function toAvro() {
    $avro = [];
    if ($this->doc !== null) {
      $avro["doc"] = $this->doc;
    }
    $avro["request"] = $this->request->toAvro();

    if ($this->isOneWay()) {
      $avro["response"] = "null";
      $avro["one-way"] = true;
    } else {
      if ($this->getResponse() !== null) {
        $avro["response"] = $this->getResponse()->getSchemaName();
      } else {
        throw new AvroProtocolParseException("Message '{$this->name}' has no declared response but is not a one-way message.");
      }
      if ($this->getErrors() !== null) {
        $avro["errors"] = [];
        foreach ($this->getErrors()->getSchemas() as $error) {
          $avro["errors"][] = $error->getSchemaName();
        }
        // default string error which is results to AvroRemoteException and is really a string type is not necessary to be declared
        array_shift($avro["errors"]);
        if (count($avro["errors"]) == 0) {
          unset($avro["errors"]);
        }
      }
    }

    return $avro;
  }
}
