<?php

declare(strict_types=1);

namespace Rabbit\DB\Redis;

use Rabbit\ActiveRecord\ActiveQueryInterface;
use Rabbit\ActiveRecord\ActiveQueryTrait;
use Rabbit\ActiveRecord\ActiveRelationTrait;
use Rabbit\Base\Exception\InvalidArgumentException;
use Rabbit\Base\Exception\NotSupportedException;
use Rabbit\DB\QueryTrait;
use Rabbit\DB\QueryTraitExt;
use Rabbit\Pool\ConnectionInterface;

class ActiveQuery implements ActiveQueryInterface
{
    use QueryTrait;
    use QueryTraitExt;
    use ActiveQueryTrait;
    use ActiveRelationTrait;

    public ?ConnectionInterface $db = null;

    public function all(): array
    {
        if ($this->emulateExecution) {
            return [];
        }

        // TODO add support for orderBy
        $models = $this->executeScript('All');
        if (empty($models)) {
            return [];
        }

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

    protected function executeScript(string $type, string $columnName = null)
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
        $modelClass = create($this->modelClass, ['db' => $this->db]);

        // find by primary key if possible. This is much faster than scanning all records
        if (is_array($this->where) && (!isset($this->where[0]) && $modelClass->isPrimaryKey(array_keys($this->where)) ||
            isset($this->where[0]) && $this->where[0] === 'in' && $modelClass->isPrimaryKey((array)$this->where[1]))) {
            return $this->findByPk($type, $columnName);
        }

        $method = 'build' . $type;
        $script = create(LuaScriptBuilder::class)->$method($this, $columnName);

        $data = $this->db->executeCommand('EVAL', [$script, 0]);
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

    public function one(): ?array
    {
        if ($this->emulateExecution) {
            return null;
        }
        $model = $this->executeScript('One');
        if (empty($model)) {
            return null;
        }
        if (!empty($this->with)) {
            $models = [$model];
            $this->findWith($this->with, $models);
            $model = $models[0];
        }

        return $model;
    }

    private function findByPk(string $type, string $columnName = null)
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
                    throw new NotSupportedException('Find by composite PK is not supported by redis ActiveRecord.');
                }
            }
            $pks = [$this->where];
        }

        /* @var $modelClass ActiveRecord */
        $modelClass = create($this->modelClass, ['db' => $this->db]);

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
        $pkey = $this->db->getCluster() ? '{' . $modelClass->keyPrefix() . '}' : $modelClass->keyPrefix();
        foreach ($pks as $pk) {
            if (++$i > $start && ($limit === null || $i <= $start + $limit)) {
                $key = $pkey . ':a:' . $modelClass->buildKey($pk);
                $result = $this->db->executeCommand('HGETALL', [$key]);
                if (!empty($result)) {
                    $data[] = $result;
                    if ($needSort) {
                        $orderArray[] = $this->db->executeCommand('HGET', [$key, $orderColumn]);
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

    public function count(string $q = '*'): int
    {
        if ($this->emulateExecution) {
            return 0;
        }

        if ($this->where === null) {
            return (int)$this->db->executeCommand('LLEN', [create($this->modelClass, ['db' => $this->db])->keyPrefix()]);
        } else {
            return (int)$this->executeScript('Count');
        }
    }

    public function exists(): bool
    {
        if ($this->emulateExecution) {
            return false;
        }
        return $this->one() !== null;
    }

    public function column(string $column): array
    {
        if ($this->emulateExecution) {
            return [];
        }
        return $this->executeScript('Column', $column);
    }

    public function sum(string $column): int
    {
        if ($this->emulateExecution) {
            return 0;
        }

        return (int)$this->executeScript('Sum', $column);
    }

    public function average(string $column): int
    {
        if ($this->emulateExecution) {
            return 0;
        }
        return (int)$this->executeScript('Average', $column);
    }

    public function min(string $column): ?int
    {
        if ($this->emulateExecution) {
            return null;
        }
        return (int)$this->executeScript('Min', $column);
    }

    public function max(string $column): ?int
    {
        if ($this->emulateExecution) {
            return null;
        }
        return (int)$this->executeScript('Max', $column);
    }

    public function scalar(string $attribute): ?string
    {
        if ($this->emulateExecution) {
            return null;
        }

        $record = $this->one();
        return $record[$attribute] ?? null;
    }
}
