<?php

declare(strict_types=1);

namespace Rabbit\DB\Redis;

use Rabbit\ActiveRecord\BaseActiveRecord;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\Inflector;
use Rabbit\Base\Helper\StringHelper;
use Rabbit\DB\StaleObjectException;
use Rabbit\Pool\AbstractConnection;
use Rabbit\Pool\ConnectionInterface;

/**
 * Class ActiveRecord
 * @package Rabbit\DB\Redis
 */
class ActiveRecord extends BaseActiveRecord
{
    public function save(bool $runValidation = true, array $attributeNames = null, float $ttl = 0): bool
    {
        if ($this->getIsNewRecord()) {
            return $this->insert($runValidation, $attributeNames, $ttl);
        }

        return $this->update($runValidation, $attributeNames, $ttl) !== 0;
    }

    public function update(bool $runValidation = true, array $attributeNames = null, float $ttl = 0): int
    {
        if ($runValidation && !$this->validate($attributeNames)) {
            return 0;
        }

        return $this->updateInternal($attributeNames, $ttl);
    }

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
        $rows = $this->updateAll($values, $condition, $ttl);

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

    public function updateAll(array $attributes, string|array $condition = '', float $ttl = 0): int
    {
        if (empty($attributes)) {
            return 0;
        }
        $n = 0;
        /** @var Redis $redis */
        $redis = $this->getDb();
        $redis(function (AbstractConnection $conn) use (&$attributes, &$condition, &$n, $ttl): void {
            $isCluster = $conn->getCluster();
            $pkey = $isCluster ? '{' . $this->keyPrefix() . '}' : $this->keyPrefix();
            $arr = $this->fetchPks($condition);
            foreach ($arr as $pk) {
                $newPk = $pk;
                $pk = $this->buildKey($pk);
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
                $newPk = $this->buildKey($newPk);
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

    private function fetchPks(string|array $condition)
    {
        $query = $this->find();
        $query->where($condition);
        $records = $query->all();
        $primaryKey = $this->primaryKey();

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

    public function find(): ActiveQuery
    {
        return create(ActiveQuery::class, ['modelClass' => get_called_class()], false);
    }

    public function updateAllCounters(array $counters, string|array $condition = '', float $ttl = 0): int
    {
        if (empty($counters)) {
            return 0;
        }
        $n = 0;
        /** @var Redis $redis */
        $redis = $this->getDb();
        $redis(function (AbstractConnection $conn) use (&$counters, &$condition, &$n, $ttl): void {
            $pkey = $conn->getCluster() ? '{' . $this->keyPrefix() . '}' : $this->keyPrefix();
            $arr = $this->fetchPks($condition);
            foreach ($arr as $pk) {
                $key = $pkey . ':a:' . $this->buildKey($pk);
                foreach ($counters as $attribute => $value) {
                    $conn->executeCommand('HINCRBY', [$key, $attribute, $value]);
                }
                $ttl && $conn->executeCommand('EXPIRE', [$key, $ttl]);
                $n++;
            }
        });
        return $n;
    }

    public function deleteAll(string|array $condition = null): int
    {
        $pks = $this->fetchPks($condition);
        if (empty($pks)) {
            return 0;
        }
        /** @var Redis $redis */
        $redis = $this->getDb();
        $result = $redis(function (AbstractConnection $conn) use (&$pks): array {
            $attributeKeys = [];
            $isCluster = $conn->getCluster();
            $pkey = $isCluster ? '{' . $this->keyPrefix() . '}' : $this->keyPrefix();
            !$isCluster && $conn->executeCommand('MULTI');
            foreach ($pks as $pk) {
                $pk = $this->buildKey($pk);
                $conn->executeCommand('LREM', [$pkey, 0, $pk]);

                $attributeKeys[] = $pkey . ':a:' . $pk;
            }
            $result = $conn->executeCommand('DEL', $attributeKeys);
            !$isCluster && ($result = $conn->executeCommand('EXEC'));
            return $result;
        });
        return (int)end($result);
    }

    public function attributes(): array
    {
        throw new InvalidConfigException('The attributes() method of redis ActiveRecord has to be implemented by child classes.');
    }

    public function insert(bool $runValidation = true, array $attributes = null, float $ttl = 0): bool
    {
        if ($runValidation && !$this->validate($attributes)) {
            return false;
        }
        $values = $this->getDirtyAttributes($attributes);
        /** @var Redis $redis */
        $redis = $this->getDb();
        $redis(function (AbstractConnection $conn) use (&$values, $ttl): void {
            $pk = [];
            $pkey = $conn->getCluster() ? '{' . $this->keyPrefix() . '}' : $this->keyPrefix();
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
            $pk = $this->buildKey($pk);
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

    public function getDb(): ConnectionInterface
    {
        return getDI('redis')->get();
    }

    public function primaryKey(): array
    {
        return ['id'];
    }

    public function keyPrefix(): string
    {
        return Inflector::camel2id(StringHelper::basename(get_called_class()), '_');
    }

    public function buildKey($key): string
    {
        if (is_numeric($key)) {
            return (string)$key;
        } elseif (is_string($key)) {
            return ctype_alnum($key) && StringHelper::byteLength($key) <= 32 ? $key : md5($key);
        } elseif (is_array($key)) {
            if (count($key) == 1) {
                return $this->buildKey(reset($key));
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
