<?php


namespace Avro\Record;

use Avro\Schema\AvroRecordSchema;

/**
 * This is a helper interface that can be used for instantiation of the custom records based on the schema and name
 * By default the same namespace and class name defined will be used and if needed it can be override by this interface
 *
 * Class IAvroRecordInstantiator
 * @package Avro\Record
 */
interface IAvroRecordInstantiator {
  function getNewRecordInstance(string $defaultNamespace, AvroRecordSchema $schema): IAvroRecordBase;
}
