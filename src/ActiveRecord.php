<?php

declare(strict_types=1);

namespace Rabbit\DB\Redis;

use DI\DependencyException;
use DI\NotFoundException;
use Psr\SimpleCache\InvalidArgumentException;
use Rabbit\ActiveRecord\BaseActiveRecord;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\Inflector;
use Rabbit\Base\Helper\StringHelper;
use Rabbit\DB\StaleObjectException;
use Rabbit\Pool\ConnectionInterface;
use Throwable;

/**
 * Class ActiveRecord
 * @package Rabbit\DB\Redis
 */
class ActiveRecord extends BaseActiveRecord
{
    /**
     * @author Albert <63851587@qq.com>
     * @param boolean $runValidation
     * @param array $attributeNames
     * @return boolean
     */
    public function save(bool $runValidation = true, array $attributeNames = null, float $ttl = 0): bool
    {
        if ($this->getIsNewRecord()) {
            return $this->insert($runValidation, $attributeNames, $ttl);
        }

        return $this->update($runValidation, $attributeNames, $ttl) !== 0;
    }
    /**
     * @author Albert <63851587@qq.com>
     * @param boolean $runValidation
     * @param array $attributeNames
     * @return integer
     */
    public function update(bool $runValidation = true, array $attributeNames = null, float $ttl = 0): int
    {
        if ($runValidation && !$this->validate($attributeNames)) {
            return 0;
        }

        return $this->updateInternal($attributeNames, $ttl);
    }
    /**
     * @author Albert <63851587@qq.com>
     * @param array $attributes
     * @return integer
     */
    protected function updateInternal(array $attributes = null, float $ttl = 0): int
    {
        $values = $this->getDirtyAttributes($attributes);
        if (empty($values)) {
            return 0;
        }
        $condition = $this->getOldPrimaryKey();
        $lock = $this->optimisticLock();
        if ($lock !== null) {
            $values[$lock] = $this->$lock + 1;
            $condition[$lock] = $this->$lock;
        }
        // We do not check the return value of updateAll() because it's possible
        // that the UPDATE statement doesn't change anything and thus returns 0.
        $rows = static::updateAll($values, $condition, $ttl);

        if ($lock !== null && !$rows) {
            throw new StaleObjectException('The object being updated is outdated.');
        }

        if (isset($values[$lock])) {
            $this->$lock = $values[$lock];
        }

        $changedAttributes = [];
        foreach ($values as $name => $value) {
            $changedAttributes[$name] = isset($this->_oldAttributes[$name]) ? $this->_oldAttributes[$name] : null;
            $this->_oldAttributes[$name] = $value;
        }

        return $rows;
    }

    /**
     * Updates the whole table using the provided attribute values and conditions.
     * For example, to change the status to be 1 for all customers whose status is 2:
     *
     * ~~~
     * Customer::updateAll(['status' => 1], ['id' => 2]);
     * ~~~
     *
     * @param array $attributes attribute values (name-value pairs) to be saved into the table
     * @param string $condition the conditions that will be put in the WHERE part of the UPDATE SQL.
     * Please refer to [[ActiveQuery::where()]] on how to specify this parameter.
     * @return int the number of rows updated
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public static function updateAll(array $attributes, $condition = '', float $ttl = 0): int
    {
        if (empty($attributes)) {
            return 0;
        }
        $n = 0;
        /** @var Redis $redis */
        $redis = static::getDb();
        $redis(function ($conn) use (&$attributes, &$condition, &$n, $ttl) {
            $isCluster = $conn->getCluster();
            $pkey = $isCluster ? '{' . static::keyPrefix() . '}' : static::keyPrefix();
            $arr = self::fetchPks($condition);
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
                        $setArgs[] = $attribute;
                        $setArgs[] = $value;
                    } else {
                        $delArgs[] = $attribute;
                    }
                }
                $newPk = static::buildKey($newPk);
                $newKey = $pkey . ':a:' . $newPk;
                // rename index if pk changed
                if ($newPk != $pk) {
                    !$isCluster && $conn->executeCommand('MULTI');
                    if (count($setArgs) > 1) {
                        $conn->executeCommand('HMSET', $setArgs);
                    }
                    if (count($delArgs) > 1) {
                        $conn->executeCommand('HDEL', $delArgs);
                    }
                    $conn->executeCommand('LINSERT', [$pkey, 'AFTER', $pk, $newPk]);
                    $conn->executeCommand('LREM', [$pkey, 0, $pk]);
                    $conn->executeCommand('RENAME', [$key, $newKey]);
                    !$isCluster && $conn->executeCommand('EXEC');
                    $ttl && $conn->executeCommand('EXPIRE', [$newKey, $ttl]);
                } else {
                    if (count($setArgs) > 1) {
                        $conn->executeCommand('HMSET', $setArgs);
                        $ttl && $conn->executeCommand('EXPIRE', [$key, $ttl]);
                    }
                    if (count($delArgs) > 1) {
                        $conn->executeCommand('HDEL', $delArgs);
                    }
                }
                $ttl && $conn->executeCommand('EXPIRE', [$key, $ttl]);
                $n++;
            }
        });
        return $n;
    }

    /**
     * @param $condition
     * @return array
     * @throws Throwable
     * @throws InvalidArgumentException
     * @throws InvalidConfigException
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
     * @throws DependencyException
     * @throws NotFoundException
     */
    public static function find(): ActiveQuery
    {
        return create(ActiveQuery::class, ['modelClass' => get_called_class()], false);
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
     * @param string $condition the conditions that will be put in the WHERE part of the UPDATE SQL.
     * Please refer to [[ActiveQuery::where()]] on how to specify this parameter.
     * @return int the number of rows updated
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public static function updateAllCounters(array $counters, $condition = '', float $ttl = 0): int
    {
        if (empty($counters)) {
            return 0;
        }
        $n = 0;
        /** @var Redis $redis */
        $redis = static::getDb();
        $redis(function ($conn) use (&$counters, &$condition, &$n, $ttl) {
            $pkey = $conn->getCluster() ? '{' . static::keyPrefix() . '}' : static::keyPrefix();
            $arr = self::fetchPks($condition);
            foreach ($arr as $pk) {
                $key = $pkey . ':a:' . static::buildKey($pk);
                foreach ($counters as $attribute => $value) {
                    $conn->executeCommand('HINCRBY', [$key, $attribute, $value]);
                }
                $ttl && $conn->executeCommand('EXPIRE', [$key, $ttl]);
                $n++;
            }
        });
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
     * @throws InvalidArgumentException
     * @throws InvalidConfigException
     * @throws Throwable
     */
    public static function deleteAll($condition = null): int
    {
        $pks = self::fetchPks($condition);
        if (empty($pks)) {
            return 0;
        }
        /** @var Redis $redis */
        $redis = static::getDb();
        $result = $redis(function ($conn) use (&$pks) {
            $attributeKeys = [];
            $isCluster = $conn->getCluster();
            $pkey = $isCluster ? '{' . static::keyPrefix() . '}' : static::keyPrefix();
            !$isCluster && $conn->executeCommand('MULTI');
            foreach ($pks as $pk) {
                $pk = static::buildKey($pk);
                $conn->executeCommand('LREM', [$pkey, 0, $pk]);

                $attributeKeys[] = $pkey . ':a:' . $pk;
            }
            $result = $conn->executeCommand('DEL', $attributeKeys);
            !$isCluster && ($result = $conn->executeCommand('EXEC'));
            return $result;
        });
        return (int)end($result);
    }

    /**
     * Returns the list of all attribute names of the model.
     * This method must be overridden by child classes to define available attributes.
     * @return array list of attribute names.
     * @throws InvalidConfigException
     */
    public function attributes(): array
    {
        throw new InvalidConfigException('The attributes() method of redis ActiveRecord has to be implemented by child classes.');
    }

    /**
     * @param bool $runValidation
     * @param array|null $attributes
     * @return bool
     * @throws Throwable
     */
    public function insert(bool $runValidation = true, array $attributes = null, float $ttl = 0): bool
    {
        if ($runValidation && !$this->validate($attributes)) {
            return false;
        }
        $values = $this->getDirtyAttributes($attributes);
        /** @var Redis $redis */
        $redis = static::getDb();
        $redis(function ($conn) use (&$values, $ttl) {
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
                    $setArgs[] = $attribute;
                    $setArgs[] = $value;
                }
            }

            if (count($setArgs) > 1) {
                $conn->executeCommand('HMSET', $setArgs);
                $ttl && $conn->executeCommand('EXPIRE', [$key, $ttl]);
            }
        });
        $this->setOldAttributes($values);

        return true;
    }

    /**
     * Returns the database connection used by this AR class.
     * By default, the "redis" application component is used as the database connection.
     * You may override this method if you want to use a different database connection.
     * @return ConnectionInterface the database connection used by this AR class.
     * @throws Throwable
     */
    public static function getDb(): ConnectionInterface
    {
        return getDI('redis')->get();
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
    public static function buildKey($key): string
    {
        if (is_numeric($key)) {
            return (string)$key;
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
