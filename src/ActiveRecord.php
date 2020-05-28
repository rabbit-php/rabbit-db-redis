<?php

namespace rabbit\db\redis;

use rabbit\activerecord\BaseActiveRecord;
use rabbit\core\ObjectFactory;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\Inflector;
use rabbit\helper\StringHelper;

/**
 * Class ActiveRecord
 * @package rabbit\db\redis
 */
class ActiveRecord extends BaseActiveRecord
{
    /**
     * Updates the whole table using the provided attribute values and conditions.
     * For example, to change the status to be 1 for all customers whose status is 2:
     *
     * ~~~
     * Customer::updateAll(['status' => 1], ['id' => 2]);
     * ~~~
     *
     * @param array $attributes attribute values (name-value pairs) to be saved into the table
     * @param array $condition the conditions that will be put in the WHERE part of the UPDATE SQL.
     * Please refer to [[ActiveQuery::where()]] on how to specify this parameter.
     * @return int the number of rows updated
     */
    public static function updateAll($attributes, $condition = null)
    {
        if (empty($attributes)) {
            return 0;
        }
        $db = static::getDb();
        $n = 0;
        $pkey = $db->getCluster() ? '{' . static::keyPrefix() . '}' : static::keyPrefix();
        $arr = self::fetchPks($condition);
        $conn = $db->getConn();
        foreach ($arr as $pk) {
            $newPk = $pk;
            $pk = static::buildKey($pk);
            $key = $pkey . ':a:' . $pk;
            // save attributes
            $delArgs = [$key];
            $setArgs = [$key];
            foreach ($attributes as $attribute => $value) {
                if (isset($newPk[$attribute])) {
                    $newPk[$attribute] = $value;
                }
                if ($value !== null) {
                    if (is_bool($value)) {
                        $value = (int)$value;
                    }
                    if ($db instanceof Redis) {
                        $setArgs[] = $attribute;
                        $setArgs[] = $value;
                    } else {
                        $setArgs[$attribute] = $value;
                    }
                } else {
                    $delArgs[] = $attribute;
                }
            }
            $newPk = static::buildKey($newPk);
            $newKey = $pkey . ':a:' . $newPk;
            // rename index if pk changed
            if ($newPk != $pk) {
                $conn->executeCommand('MULTI');
                if (count($setArgs) > 1) {
                    if ($db instanceof Redis) {
                        $conn->executeCommand('HMSET', $setArgs);
                    } else {
                        $hash = array_shift($setArgs);
                        $conn->executeCommand('HMSET', [$hash, $setArgs]);
                    }
                }
                if (count($delArgs) > 1) {
                    if ($db instanceof Redis) {
                        $conn->executeCommand('HDEL', $delArgs);
                    } else {
                        $hash = array_shift($setArgs);
                        $conn->executeCommand('HDEL', [$hash, implode(' ', $delArgs)]);
                    }
                }
                $conn->executeCommand('LINSERT', [$pkey, 'AFTER', $pk, $newPk]);
                if ($db instanceof Redis) {
                    $conn->executeCommand('LREM', [$pkey, 0, $pk]);
                } else {
                    $conn->executeCommand('LREM', [$pkey, $pk, 0]);
                }
                $conn->executeCommand('RENAME', [$key, $newKey]);
                $conn->executeCommand('EXEC');
            } else {
                if (count($setArgs) > 1) {
                    if ($db instanceof Redis) {
                        $conn->executeCommand('HMSET', $setArgs);
                    } else {
                        $hash = array_shift($setArgs);
                        $conn->executeCommand('HMSET', [$hash, $setArgs]);
                    }
                }
                if (count($delArgs) > 1) {
                    if ($db instanceof Redis) {
                        $conn->executeCommand('HDEL', $delArgs);
                    } else {
                        $hash = array_shift($setArgs);
                        $conn->executeCommand('HDEL', [$hash, implode(' ', $delArgs)]);
                    }
                }
            }
            $n++;
        }
        $conn->release(true);
        return $n;
    }

    /**
     * @param $condition
     * @return array
     */
    private static function fetchPks($condition)
    {
        $query = static::find();
        $query->where($condition);
        $records = $query->asArray()->all(); // TODO limit fetched columns to pk
        $primaryKey = static::primaryKey();

        $pks = [];
        foreach ($records as $record) {
            $pk = [];
            foreach ($primaryKey as $key) {
                $pk[$key] = $record[$key];
            }
            $pks[] = $pk;
        }

        return $pks;
    }

    /**
     * @inheritdoc
     * @return ActiveQuery the newly created [[ActiveQuery]] instance.
     */
    public static function find(): ActiveQuery
    {
        return ObjectFactory::createObject(ActiveQuery::class, ['modelClass' => get_called_class()], false);
    }

    /**
     * Updates the whole table using the provided counter changes and conditions.
     * For example, to increment all customers' age by 1,
     *
     * ~~~
     * Customer::updateAllCounters(['age' => 1]);
     * ~~~
     *
     * @param array $counters the counters to be updated (attribute name => increment value).
     * Use negative values if you want to decrement the counters.
     * @param array $condition the conditions that will be put in the WHERE part of the UPDATE SQL.
     * Please refer to [[ActiveQuery::where()]] on how to specify this parameter.
     * @return int the number of rows updated
     */
    public static function updateAllCounters($counters, $condition = null)
    {
        if (empty($counters)) {
            return 0;
        }
        $db = static::getDb();
        $n = 0;
        $pkey = $db->getCluster() ? '{' . static::keyPrefix() . '}' : static::keyPrefix();
        $arr = self::fetchPks($condition);
        $conn = $db->getConn();
        foreach ($arr as $pk) {
            $key = $pkey . ':a:' . static::buildKey($pk);
            foreach ($counters as $attribute => $value) {
                $conn->executeCommand('HINCRBY', [$key, $attribute, $value]);
            }
            $n++;
        }
        $conn->release(true);
        return $n;
    }

    /**
     * Deletes rows in the table using the provided conditions.
     * WARNING: If you do not specify any condition, this method will delete ALL rows in the table.
     *
     * For example, to delete all customers whose status is 3:
     *
     * ~~~
     * Customer::deleteAll(['status' => 3]);
     * ~~~
     *
     * @param array $condition the conditions that will be put in the WHERE part of the DELETE SQL.
     * Please refer to [[ActiveQuery::where()]] on how to specify this parameter.
     * @return int the number of rows deleted
     */
    public static function deleteAll($condition = null)
    {
        $pks = self::fetchPks($condition);
        if (empty($pks)) {
            return 0;
        }

        $db = static::getDb();
        $conn = $db->getConn();
        $attributeKeys = [];
        $pkey = $conn->getCluster() ? '{' . static::keyPrefix() . '}' : static::keyPrefix();
        $conn->executeCommand('MULTI');
        foreach ($pks as $pk) {
            $pk = static::buildKey($pk);
            if ($db instanceof Redis) {
                $conn->executeCommand('LREM', [$pkey, 0, $pk]);
            } else {
                $conn->executeCommand('LREM', [$pkey, $pk, 0]);
            }

            $attributeKeys[] = $pkey . ':a:' . $pk;
        }
        $conn->executeCommand('DEL', $attributeKeys);
        $result = $conn->executeCommand('EXEC');
        $conn->release(true);

        return end($result);
    }

    /**
     * Returns the list of all attribute names of the model.
     * This method must be overridden by child classes to define available attributes.
     * @return array list of attribute names.
     */
    public function attributes(): array
    {
        throw new InvalidConfigException('The attributes() method of redis ActiveRecord has to be implemented by child classes.');
    }

    /**
     * @inheritdoc
     */
    public function insert($runValidation = true, $attributes = null)
    {
        if ($runValidation && !$this->validate($attributes)) {
            return false;
        }
        $db = static::getDb();
        $conn = $db->getConn();
        $values = $this->getDirtyAttributes($attributes);
        $pk = [];
        $pkey = $conn->getCluster() ? '{' . static::keyPrefix() . '}' : static::keyPrefix();
        foreach ($this->primaryKey() as $key) {
            $pk[$key] = $values[$key] = $this->getAttribute($key);
            if ($pk[$key] === null) {
                // use auto increment if pk is null
                $pk[$key] = $values[$key] = $conn->executeCommand('INCR', [$pkey . ':s:' . $key]);
                $this->setAttribute($key, $values[$key]);
            } elseif (is_numeric($pk[$key])) {
                // if pk is numeric update auto increment value
                $currentPk = $conn->executeCommand('GET', [$pkey . ':s:' . $key]);
                if ($pk[$key] > $currentPk) {
                    $conn->executeCommand('SET', [$pkey . ':s:' . $key, $pk[$key]]);
                }
            }
        }
        // save pk in a findall pool
        $pk = static::buildKey($pk);
        $conn->executeCommand('RPUSH', [$pkey, $pk]);

        $key = $pkey . ':a:' . $pk;
        // save attributes
        $setArgs = [$key];
        foreach ($values as $attribute => $value) {
            // only insert attributes that are not null
            if ($value !== null) {
                if (is_bool($value)) {
                    $value = (int)$value;
                }
                if ($db instanceof Redis) {
                    $setArgs[] = $attribute;
                    $setArgs[] = $value;
                } else {
                    $setArgs[$attribute] = $value;
                }
            }
        }

        if (count($setArgs) > 1) {
            if ($db instanceof Redis) {
                $conn->executeCommand('HMSET', $setArgs);
            } else {
                $hash = array_shift($setArgs);
                $conn->executeCommand('HMSET', [$hash, $setArgs]);
            }
        }
        $conn->release(true);
        $changedAttributes = array_fill_keys(array_keys($values), null);
        $this->setOldAttributes($values);

        return true;
    }

    /**
     * Returns the database connection used by this AR class.
     * By default, the "redis" application component is used as the database connection.
     * You may override this method if you want to use a different database connection.
     * @return Connection the database connection used by this AR class.
     */
    public static function getDb()
    {
        return getDI('redis');
    }

    /**
     * Returns the primary key name(s) for this AR class.
     * This method should be overridden by child classes to define the primary key.
     *
     * Note that an array should be returned even when it is a single primary key.
     *
     * @return string[] the primary keys of this record.
     */
    public static function primaryKey(): array
    {
        return ['id'];
    }

    /**
     * Declares prefix of the key that represents the keys that store this records in redis.
     * By default this method returns the class name as the table name by calling [[Inflector::camel2id()]].
     * For example, 'Customer' becomes 'customer', and 'OrderItem' becomes
     * 'order_item'. You may override this method if you want different key naming.
     * @return string the prefix to apply to all AR keys
     */
    public static function keyPrefix(): string
    {
        return Inflector::camel2id(StringHelper::basename(get_called_class()), '_');
    }

    /**
     * Builds a normalized key from a given primary key value.
     *
     * @param mixed $key the key to be normalized
     * @return string the generated key
     */
    public static function buildKey($key)
    {
        if (is_numeric($key)) {
            return $key;
        } elseif (is_string($key)) {
            return ctype_alnum($key) && StringHelper::byteLength($key) <= 32 ? $key : md5($key);
        } elseif (is_array($key)) {
            if (count($key) == 1) {
                return self::buildKey(reset($key));
            }
            ksort($key); // ensure order is always the same
            $isNumeric = true;
            foreach ($key as $value) {
                if (!is_numeric($value)) {
                    $isNumeric = false;
                }
            }
            if ($isNumeric) {
                return implode('-', $key);
            }
        }

        return md5(json_encode($key, JSON_NUMERIC_CHECK));
    }
}
