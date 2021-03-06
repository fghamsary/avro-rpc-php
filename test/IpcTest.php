<?php
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use Avro\Exception\AvroException;
use Avro\Exception\AvroSchemaParseException;
use Avro\Protocol\AvroProtocol;
use Avro\Exception\AvroRemoteException;
use Avro\IPC\Requester;
use Avro\IPC\Responder;
use Avro\IPC\SocketTransceiver;
use Avro\Protocol\AvroProtocolMessage;
use Avro\Protocol\AvroProtocolParseException;
use Avro\Test\AlwaysRaised;

require_once('test_helper.php');
require_once 'AlwaysRaised.php';

/**
 * Basic Transceiver to connect to a TestServer
 * (no transport involved)
 */
class TestTransceiver extends SocketTransceiver {

  /**
   * @var TestServer
   */
  public $server = null;
  protected $response = null;

  public static function getTestClient(TestServer $server) {
    $transceiver = new TestTransceiver();
    $transceiver->server = $server;

    return $transceiver;
  }

  public function readMessage() {
    return $this->response;
  }

  /**
   * Writes a message into the channel. Blocks until the message has been written.
   * @param string $message
   * @throws AvroException
   */
  public function writeMessage($message) {
    $this->response = null;
    if ($this->server !== null) {
      $this->response = $this->server->start($message);
    }
  }

  /**
   * Return the name of the socket remode side
   * @return string the remote name
   */
  public function remoteName() {
    return 'TestTransceiver';
  }
}

/**
 * Basic test server that only call it's responder invoke function
 */
class TestServer {

  /**
   * @var Responder
   */
  public $responder;

  /**
   * @var TestTransceiver
   */
  public $transceiver;

  public function __construct(Responder $responder) {
    $this->responder = $responder;
    $this->transceiver = new TestTransceiver();
  }

  /**
   * @param $callRequest
   * @return string|null
   * @throws AvroException
   */
  public function start($callRequest) {
    $callResponse = $this->responder->respond($callRequest, $this->transceiver);
    return $callResponse;
  }
}


class TestProtocolResponder extends Responder {

  public function invoke(AvroProtocolMessage $localMessage, $request) {
    switch ($localMessage->getName()) {
      case "testSimpleRequestResponse":
        if ($request["message"]["subject"] == "ping") {
          return ["response" => "pong"];
        } else {
          if ($request["message"]["subject"] == "pong") {
            return ["response" => "ping"];
          } else {
            return ["response" => "no Idea what to say"];
          }
        }
        break;

      case "testSimpleRequestParams":
        return ["response" => "ping"];

      case "testSimpleRequestWithoutParameters":
        return ["response" => "no incoming parameters"];

      case "testNotification":
        return null;

      case "testRequestResponseException":
        if ($request["exception"]["cause"] == "callback") {
          throw new AlwaysRaised("raised on callback cause");
        } else {
          throw new AvroRemoteException("System exception");
        }
        break;

      default:
        throw new AvroRemoteException("Method unknown");
    }
  }
}


class IpcTest extends PHPUnit\Framework\TestCase {

  /**
   * @throws AvroException
   * @throws AvroProtocolParseException
   * @throws AvroRemoteException
   * @throws AvroSchemaParseException
   */
  public function testSimpleRequestResponse() {
    $server = new TestServer(new TestProtocolResponder(AvroProtocol::parse($this->protocol)));
    $client = TestTransceiver::getTestClient($server);
    $requester = new Requester(AvroProtocol::parse($this->protocol), $client);

    $response = $requester->request('testSimpleRequestResponse', ["message" => ["subject" => "ping"]]);
    $this->assertEquals("pong", $response["response"]);
    $response = $requester->request('testSimpleRequestResponse', ["message" => ["subject" => "pong"]]);
    $this->assertEquals("ping", $response["response"]);

  }

  /**
   * @throws AvroException
   * @throws AvroProtocolParseException
   * @throws AvroRemoteException
   * @throws AvroSchemaParseException
   */
  public function testNotification() {
    $server = new TestServer(new TestProtocolResponder(AvroProtocol::parse($this->protocol)));
    $client = TestTransceiver::getTestClient($server);
    $requester = new Requester(AvroProtocol::parse($this->protocol), $client);

    $response = $requester->request('testNotification', ["notification" => ["subject" => "notify"]]);
    $this->assertTrue($response);
  }

  /**
   * @throws AvroRemoteException
   * @throws AvroException
   * @throws AvroSchemaParseException
   * @throws AvroProtocolParseException
   */
  public function testHandshake() {
    $server = new TestServer(new TestProtocolResponder(AvroProtocol::parse($this->protocol)));
    $client = TestTransceiver::getTestClient($server);
    $requester = new Requester(AvroProtocol::parse($this->protocol), $client);

    $this->assertFalse($client->isConnected());
    $this->assertFalse($server->transceiver->isConnected());

    $response = $requester->request('testNotification', ["notification" => ["subject" => "notify"]]);
    $this->assertTrue($response);
    $this->assertTrue($client->isConnected());
    $this->assertTrue($server->transceiver->isConnected());

    $response = $requester->request('testSimpleRequestResponse', ["message" => ["subject" => "ping"]]);
    $this->assertEquals("pong", $response["response"]);
    $this->assertTrue($client->isConnected());
    $this->assertTrue($server->transceiver->isConnected());
  }

  /**
   * @throws AvroRemoteException
   * @throws AvroException
   * @throws AvroSchemaParseException
   * @throws AvroProtocolParseException
   */
  public function testRequestResponseException() {
    $server = new TestServer(new TestProtocolResponder(AvroProtocol::parse($this->protocol)));
    $client = TestTransceiver::getTestClient($server);
    $requester = new Requester(AvroProtocol::parse($this->protocol), $client);

    $exceptionRaised = false;
    try {
      $response = $requester->request('testRequestResponseException', ["exception" => ["cause" => "callback"]]);
      $this->assertNull($response);
    } catch (AlwaysRaised $e) {
      $exceptionRaised = true;
      $exceptionDatum = $e->getException();
      $this->assertEquals("raised on callback cause", $exceptionDatum);
    }

    $this->assertTrue($exceptionRaised);
    $exceptionRaised = false;
    try {
      $response = $requester->request('testRequestResponseException', ["exception" => ["cause" => "system"]]);
      $this->assertNull($response);
    } catch (AvroRemoteException $e) {
      $exceptionRaised = true;
      $exceptionDatum = $e->getMessage();
      $this->assertEquals("System exception", $exceptionDatum);
    }
    $this->assertTrue($exceptionRaised);
  }

  private $protocol = <<<PROTO
{
 "namespace": "avro.test",
 "protocol": "TestProtocol",

 "types": [
     {"type": "record", "name": "SimpleRequest",
      "fields": [{"name": "subject",   "type": "string"}]
     },
     {"type": "record", "name": "SimpleResponse",
      "fields": [{"name": "response",   "type": "string"}]
     },
     {"type": "record", "name": "Notification",
      "fields": [{"name": "subject",   "type": "string"}]
     },
     {"type": "record", "name": "RaiseException",
      "fields": [{"name": "cause",   "type": "string"}]
     },
     {"type": "record", "name": "NeverSend",
      "fields": [{"name": "never",   "type": "string"}]
     },
     {"type": "error", "name": "AlwaysRaised",
      "fields": [{"name": "exception",   "type": "string"}]
     }
 ],

 "messages": {
     "testSimpleRequestResponse": {
         "doc" : "Simple Request Response",
         "request": [{"name": "message", "type": "SimpleRequest"}],
         "response": "SimpleResponse"
     },
     "testSimpleRequestWithoutParameters": {
         "doc" : "Simple Request Response",
         "request": [],
         "response": "SimpleResponse"
     },
     "testNotification": {
         "doc" : "Notification : one-way message",
         "request": [{"name": "notification", "type": "Notification"}],
         "one-way": true
     },
     "testRequestResponseException": {
         "doc" : "Request Response with Exception",
         "request": [{"name": "exception", "type": "RaiseException"}],
         "response" : "NeverSend",
         "errors" : ["AlwaysRaised"]
     }
 }
}

PROTO;

}