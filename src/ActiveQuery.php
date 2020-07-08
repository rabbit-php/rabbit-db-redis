<?php
declare(strict_types=1);

namespace Rabbit\DB\Redis;

use Rabbit\ActiveRecord\ActiveQueryInterface;
use Rabbit\ActiveRecord\ActiveQueryTrait;
use Rabbit\ActiveRecord\ActiveRelationTrait;
use Rabbit\Base\Exception\InvalidArgumentException;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Exception\NotSupportedException;
use Rabbit\DB\QueryTrait;
use Rabbit\DB\QueryTraitExt;
use Rabbit\Pool\ConnectionInterface;
use ReflectionException;
use Throwable;

/**
 * Class ActiveQuery
 * @package Rabbit\DB\Redis
 */
class ActiveQuery implements ActiveQueryInterface
{
    use QueryTrait;
    use QueryTraitExt;
    use ActiveQueryTrait;
    use ActiveRelationTrait;

    /**
     * Constructor.
     * @param string $modelClass the model class associated with this query
     * @param array $config configurations to be applied to the newly created query object
     */
    public function __construct(string $modelClass, array $config = [])
    {
        $this->modelClass = $modelClass;
    }

    /**
     * Executes the query and returns all results as an array.
     * @param ConnectionInterface $db the database connection used to execute the query.
     * If this parameter is not given, the `db` application component will be used.
     * @return array|ActiveRecord[] the query results. If the query results in nothing, an empty array will be returned.
     * @throws InvalidConfigException
     * @throws Throwable
     * @throws \Psr\SimpleCache\InvalidArgumentException
     */
    public function all(ConnectionInterface $db = null): array
    {
        if ($this->emulateExecution) {
            return [];
        }

        // TODO add support for orderBy
        $rows = $this->executeScript($db, 'All');
        if (empty($rows)) {
            return [];
        }

        $models = $this->createModels($rows);
        if (!empty($this->with)) {
            $this->findWith($this->with, $models);
        }
        if ($this->indexBy !== null) {
            $indexedModels = [];
            if (is_string($this->indexBy)) {
                foreach ($models as $model) {
                    $key = $model[$this->indexBy];
                    $indexedModels[$key] = $model;
                }
            } else {
                foreach ($models as $model) {
                    $key = call_user_func($this->indexBy, $model);
                    $indexedModels[$key] = $model;
                }
            }
            $models = $indexedModels;
        }

        return $models;
    }

    /**
     * Executes a script created by [[LuaScriptBuilder]]
     * @param ConnectionInterface $db the database connection used to execute the query.
     * If this parameter is not given, the `db` application component will be used.
     * @param string $type the type of the script to generate
     * @param string $columnName
     * @return array|bool|null|string
     * @throws InvalidConfigException
     * @throws ReflectionException
     * @throws Throwable
     * @throws \Psr\SimpleCache\InvalidArgumentException
     */
    protected function executeScript(?ConnectionInterface $db, string $type, string $columnName = null)
    {
        if ($this->primaryModel !== null) {
            // lazy loading
            if ($this->via instanceof self) {
                // via junction table
                $viaModels = $this->via->findJunctionRows([$this->primaryModel]);
                $this->filterByModels($viaModels);
            } elseif (is_array($this->via)) {
                // via relation
                /* @var $viaQuery ActiveQuery */
                list($viaName, $viaQuery) = $this->via;
                if ($viaQuery->multiple) {
                    $viaModels = $viaQuery->all();
                    $this->primaryModel->populateRelation($viaName, $viaModels);
                } else {
                    $model = $viaQuery->one();
                    $this->primaryModel->populateRelation($viaName, $model);
                    $viaModels = $model === null ? [] : [$model];
                }
                $this->filterByModels($viaModels);
            } else {
                $this->filterByModels([$this->primaryModel]);
            }
        }

        /* @var $modelClass ActiveRecord */
        $modelClass = $this->modelClass;

        if ($db === null) {
            $db = $modelClass::getDb();
        }

        // find by primary key if possible. This is much faster than scanning all records
        if (is_array($this->where) && (
                !isset($this->where[0]) && $modelClass::isPrimaryKey(array_keys($this->where)) ||
                isset($this->where[0]) && $this->where[0] === 'in' && $modelClass::isPrimaryKey((array)$this->where[1])
            )) {
            return $this->findByPk($db, $type, $columnName);
        }

        $method = 'build' . $type;
        $script = (new LuaScriptBuilder())->$method($this, $columnName);

        $data = $db->executeCommand('EVAL', [$script, 0]);
        if (is_array($data)) {
            switch ($type) {
                case 'All':
                    $rows = [];
                    foreach ($data as $item) {
                        $row = [];
                        $c = count($item);
                        for ($i = 0; $i < $c;) {
                            $row[$item[$i++]] = $item[$i++];
                        }
                        $rows[] = $row;
                    }
                    return $rows;
                case 'One':
                    $row = [];
                    $c = count($data);
                    for ($i = 0; $i < $c;) {
                        $row[$data[$i++]] = $data[$i++];
                    }
                    return $row;
                case 'Count':
                    return count($data);
                case 'Column':
                    $column = [];
                    foreach ($data as $dataRow) {
                        $row = [];
                        $c = count($dataRow);
                        for ($i = 0; $i < $c;) {
                            $row[$dataRow[$i++]] = $dataRow[$i++];
                        }
                        $column[] = $row[$columnName];
                    }
                    return $column;
                case 'Sum':
                    $sum = 0;
                    foreach ($data as $dataRow) {
                        $c = count($dataRow);
                        for ($i = 0; $i < $c;) {
                            if ($dataRow[$i++] == $columnName) {
                                $sum += $dataRow[$i];
                                break;
                            }
                        }
                    }
                    return $sum;
                case 'Average':
                    $sum = 0;
                    $count = 0;
                    foreach ($data as $dataRow) {
                        $count++;
                        $c = count($dataRow);
                        for ($i = 0; $i < $c;) {
                            if ($dataRow[$i++] == $columnName) {
                                $sum += $dataRow[$i];
                                break;
                            }
                        }
                    }

                    return $sum / $count;
                case 'Min':
                    $min = null;
                    foreach ($data as $dataRow) {
                        $c = count($dataRow);
                        for ($i = 0; $i < $c;) {
                            if ($dataRow[$i++] == $columnName && ($min == null || $dataRow[$i] < $min)) {
                                $min = $dataRow[$i];
                                break;
                            }
                        }
                    }

                    return $min;
                case 'Max':
                    $max = null;
                    foreach ($data as $dataRow) {
                        $c = count($dataRow);
                        for ($i = 0; $i < $c;) {
                            if ($dataRow[$i++] == $columnName && ($max == null || $dataRow[$i] > $max)) {
                                $max = $dataRow[$i];
                                break;
                            }
                        }
                    }
                    return $max;
            }
            throw new InvalidArgumentException('Unknown fetch type: ' . $type);
        }
        return $data;
    }

    /**
     * Executes the query and returns a single row of result.
     * @param ConnectionInterface $db the database connection used to execute the query.
     * If this parameter is not given, the `db` application component will be used.
     * @return ActiveRecord|array|null a single row of query result. Depending on the setting of [[asArray]],
     * the query result may be either an array or an ActiveRecord object. Null will be returned
     * if the query results in nothing.
     * @throws InvalidConfigException
     * @throws ReflectionException
     * @throws Throwable
     * @throws \Psr\SimpleCache\InvalidArgumentException
     */
    public function one(ConnectionInterface $db = null)
    {
        if ($this->emulateExecution) {
            return null;
        }

        // TODO add support for orderBy
        $row = $this->executeScript($db, 'One');
        if (empty($row)) {
            return null;
        }
        if ($this->asArray) {
            $model = $row;
        } else {
            /* @var $class ActiveRecord */
            $class = $this->modelClass;
            $model = $class::instantiate($row);
            $class = get_class($model);
            $class::populateRecord($model, $row);
        }
        if (!empty($this->with)) {
            $models = [$model];
            $this->findWith($this->with, $models);
            $model = $models[0];
        }

        return $model;
    }

    /**
     * Fetch by pk if possible as this is much faster
     * @param ConnectionInterface $db the database connection used to execute the query.
     * If this parameter is not given, the `db` application component will be used.
     * @param string $type the type of the script to generate
     * @param string $columnName
     * @return array|bool|null|string
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    private function findByPk(ConnectionInterface $db, string $type, string $columnName = null)
    {
        $needSort = !empty($this->orderBy) && in_array($type, ['All', 'One', 'Column']);
        if ($needSort) {
            if (!is_array($this->orderBy) || count($this->orderBy) > 1) {
                throw new NotSupportedException(
                    'orderBy by multiple columns is not currently supported by redis ActiveRecord.'
                );
            }

            $k = key($this->orderBy);
            $v = $this->orderBy[$k];
            if (is_numeric($k)) {
                $orderColumn = $v;
                $orderType = SORT_ASC;
            } else {
                $orderColumn = $k;
                $orderType = $v;
            }
        }

        if (isset($this->where[0]) && $this->where[0] === 'in') {
            $pks = (array)$this->where[2];
        } elseif (count($this->where) == 1) {
            $pks = (array)reset($this->where);
        } else {
            foreach ($this->where as $values) {
                if (is_array($values)) {
                    // TODO support composite IN for composite PK
                    throw new NotSupportedException('Find by composite PK is not supported by redis ActiveRecord.');
                }
            }
            $pks = [$this->where];
        }

        /* @var $modelClass ActiveRecord */
        $modelClass = $this->modelClass;

        if ($type === 'Count') {
            $start = 0;
            $limit = null;
        } else {
            $start = ($this->offset === null || $this->offset < 0) ? 0 : $this->offset;
            $limit = ($this->limit < 0) ? null : $this->limit;
        }
        $i = 0;
        $data = [];
        $orderArray = [];
        $pkey = $db->getCluster() ? '{' . $modelClass::keyPrefix() . '}' : $modelClass::keyPrefix();
        foreach ($pks as $pk) {
            if (++$i > $start && ($limit === null || $i <= $start + $limit)) {
                $key = $pkey . ':a:' . $modelClass::buildKey($pk);
                $result = $db->executeCommand('HGETALL', [$key]);
                if (!empty($result)) {
                    $data[] = $result;
                    if ($needSort) {
                        $orderArray[] = $db->executeCommand('HGET', [$key, $orderColumn]);
                    }
                    if ($type === 'One' && $this->orderBy === null) {
                        break;
                    }
                }
            }
        }

        if ($needSort) {
            $resultData = [];
            if ($orderType === SORT_ASC) {
                asort($orderArray, SORT_NATURAL);
            } else {
                arsort($orderArray, SORT_NATURAL);
            }
            foreach ($orderArray as $orderKey => $orderItem) {
                $resultData[] = $data[$orderKey];
            }
            $data = $resultData;
        }

        switch ($type) {
            case 'All':
                return $data;
            case 'One':
                return reset($data);
            case 'Count':
                return count($data);
            case 'Column':
                $column = [];
                foreach ($data as $dataRow) {
                    $column[] = $dataRow[$columnName];
                }

                return $column;
            case 'Sum':
                $sum = 0;
                foreach ($data as $dataRow) {
                    $sum += $dataRow[$columnName];
                }

                return $sum;
            case 'Average':
                $sum = 0;
                $count = count($data);
                foreach ($data as $dataRow) {
                    $sum += $dataRow[$columnName];
                }

                return $sum / $count;
            case 'Min':
                $min = null;
                foreach ($data as $dataRow) {
                    if ($min === null || $dataRow[$columnName] < $min) {
                        $min = $dataRow[$columnName];
                    }
                }

                return $min;
            case 'Max':
                $max = null;
                foreach ($data as $dataRow) {
                    if ($dataRow[$columnName] > $max) {
                        $max = $dataRow[$columnName];
                    }
                }

                return $max;
        }
        throw new InvalidArgumentException('Unknown fetch type: ' . $type);
    }

    /**
     * Returns the number of records.
     * @param string $q the COUNT expression. This parameter is ignored by this implementation.
     * @param ConnectionInterface $db the database connection used to execute the query.
     * If this parameter is not given, the `db` application component will be used.
     * @return int number of records
     * @throws InvalidConfigException
     * @throws ReflectionException
     * @throws Throwable
     * @throws \Psr\SimpleCache\InvalidArgumentException
     */
    public function count(string $q = '*', ConnectionInterface $db = null): int
    {
        if ($this->emulateExecution) {
            return 0;
        }

        if ($this->where === null) {
            /* @var $modelClass ActiveRecord */
            $modelClass = $this->modelClass;
            if ($db === null) {
                $db = $modelClass::getDb();
            }
            return (int)$db->executeCommand('LLEN', [$modelClass::keyPrefix()]);
        } else {
            return (int)$this->executeScript($db, 'Count');
        }
    }

    /**
     * Returns a value indicating whether the query result contains any row of data.
     * @param ConnectionInterface $db the database connection used to execute the query.
     * If this parameter is not given, the `db` application component will be used.
     * @return bool whether the query result contains any row of data.
     * @throws InvalidConfigException
     * @throws ReflectionException
     * @throws Throwable
     * @throws \Psr\SimpleCache\InvalidArgumentException
     */
    public function exists(ConnectionInterface $db = null): bool
    {
        if ($this->emulateExecution) {
            return false;
        }
        return $this->one($db) !== null;
    }

    /**
     * Executes the query and returns the first column of the result.
     * @param string $column name of the column to select
     * @param ConnectionInterface $db the database connection used to execute the query.
     * If this parameter is not given, the `db` application component will be used.
     * @return array the first column of the query result. An empty array is returned if the query results in nothing.
     * @throws InvalidConfigException
     * @throws ReflectionException
     * @throws Throwable
     * @throws \Psr\SimpleCache\InvalidArgumentException
     */
    public function column(string $column, ConnectionInterface $db = null): array
    {
        if ($this->emulateExecution) {
            return [];
        }

        // TODO add support for orderBy
        return $this->executeScript($db, 'Column', $column);
    }

    /**
     * Returns the number of records.
     * @param string $column the column to sum up
     * @param ConnectionInterface $db the database connection used to execute the query.
     * If this parameter is not given, the `db` application component will be used.
     * @return int number of records
     * @throws InvalidConfigException
     * @throws ReflectionException
     * @throws Throwable
     * @throws \Psr\SimpleCache\InvalidArgumentException
     */
    public function sum(string $column, ConnectionInterface $db = null): int
    {
        if ($this->emulateExecution) {
            return 0;
        }

        return $this->executeScript($db, 'Sum', $column);
    }

    /**
     * Returns the average of the specified column values.
     * @param string $column the column name or expression.
     * Make sure you properly quote column names in the expression.
     * @param ConnectionInterface $db the database connection used to execute the query.
     * If this parameter is not given, the `db` application component will be used.
     * @return int the average of the specified column values.
     * @throws InvalidConfigException
     * @throws ReflectionException
     * @throws Throwable
     * @throws \Psr\SimpleCache\InvalidArgumentException
     */
    public function average(string $column, ConnectionInterface $db = null): int
    {
        if ($this->emulateExecution) {
            return 0;
        }
        return $this->executeScript($db, 'Average', $column);
    }

    /**
     * Returns the minimum of the specified column values.
     * @param string $column the column name or expression.
     * Make sure you properly quote column names in the expression.
     * @param ConnectionInterface $db the database connection used to execute the query.
     * If this parameter is not given, the `db` application component will be used.
     * @return int the minimum of the specified column values.
     * @throws InvalidConfigException
     * @throws ReflectionException
     * @throws Throwable
     * @throws \Psr\SimpleCache\InvalidArgumentException
     */
    public function min(string $column, ConnectionInterface $db = null): ?int
    {
        if ($this->emulateExecution) {
            return null;
        }
        return $this->executeScript($db, 'Min', $column);
    }

    /**
     * Returns the maximum of the specified column values.
     * @param string $column the column name or expression.
     * Make sure you properly quote column names in the expression.
     * @param ConnectionInterface $db the database connection used to execute the query.
     * If this parameter is not given, the `db` application component will be used.
     * @return int the maximum of the specified column values.
     * @throws InvalidConfigException
     * @throws ReflectionException
     * @throws Throwable
     * @throws \Psr\SimpleCache\InvalidArgumentException
     */
    public function max(string $column,ConnectionInterface $db = null): ?int
    {
        if ($this->emulateExecution) {
            return null;
        }
        return $this->executeScript($db, 'Max', $column);
    }

    /**
     * Returns the query result as a scalar value.
     * The value returned will be the specified attribute in the first record of the query results.
     * @param string $attribute name of the attribute to select
     * @param ConnectionInterface $db the database connection used to execute the query.
     * If this parameter is not given, the `db` application component will be used.
     * @return string the value of the specified attribute in the first record of the query result.
     * Null is returned if the query result is empty.
     * @throws InvalidConfigException
     * @throws ReflectionException
     * @throws Throwable
     * @throws \Psr\SimpleCache\InvalidArgumentException
     */
    public function scalar(string $attribute,ConnectionInterface $db = null): ?string
    {
        if ($this->emulateExecution) {
            return null;
        }

        $record = $this->one($db);
        if ($record !== null) {
            return $record->hasAttribute($attribute) ? $record->$attribute : null;
        } else {
            return null;
        }
    }
}
