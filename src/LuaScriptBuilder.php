<?php

declare(strict_types=1);

namespace Rabbit\DB\Redis;

use Rabbit\Base\Exception\InvalidArgumentException;
use Rabbit\Base\Exception\NotSupportedException;
use Rabbit\DB\Exception;
use Rabbit\DB\Expression;

/**
 * Class LuaScriptBuilder
 * @package Rabbit\DB\Redis
 */
class LuaScriptBuilder
{
    public function buildAll(ActiveQuery $query): string
    {
        $key = $this->getKey($query);
        return $this->build($query, "n=n+1 pks[n]=redis.call('HGETALL',$key .. pk)", 'pks');
    }

    private function quoteValue($str): string
    {
        if (!is_string($str) && !is_int($str)) {
            return $str;
        }

        return "'" . addcslashes((string)$str, "\000\n\r\\\032\047") . "'";
    }

    private function build(ActiveQuery $query, string $buildResult, string $return): string
    {
        $columns = [];
        if ($query->where !== null) {
            $condition = $this->buildCondition($query->where, $columns);
        } else {
            $condition = 'true';
        }

        $start = ($query->offset === null || $query->offset < 0) ? 0 : $query->offset;
        $limitCondition = 'i>' . $start . (($query->limit === null || $query->limit < 0) ? '' : ' and i<=' . ($start + $query->limit));
        $key = $this->getKey($query);
        $loadColumnValues = '';
        foreach ($columns as $column => $alias) {
            $loadColumnValues .= "local $alias=redis.call('HGET',$key .. ':a:' .. pk, " . $this->quoteValue($column) . ")\n";
        }

        $getAllPks = <<<EOF
local allpks=redis.call('LRANGE',$key,0,-1)
EOF;
        if (!empty($query->orderBy)) {
            if (!is_array($query->orderBy) || count($query->orderBy) > 1) {
                throw new NotSupportedException(
                    'orderBy by multiple columns is not currently supported by redis ActiveRecord.'
                );
            }

            $k = key($query->orderBy);
            $v = $query->orderBy[$k];
            if (is_numeric($k)) {
                $orderColumn = $v;
                $orderType = 'ASC';
            } else {
                $orderColumn = $k;
                $orderType = $v === SORT_DESC ? 'DESC' : 'ASC';
            }

            $getAllPks = <<<EOF
local allpks=redis.pcall('SORT', $key, 'BY', $key .. ':a:*->' .. '$orderColumn', '$orderType')
if allpks['err'] then
    allpks=redis.pcall('SORT', $key, 'BY', $key .. ':a:*->' .. '$orderColumn', '$orderType', 'ALPHA')
end
EOF;
        }

        return <<<EOF
$getAllPks
local pks={}
local n=0
local v=nil
local i=0
local key=$key
for k,pk in ipairs(allpks) do
    $loadColumnValues
    if $condition then
      i=i+1
      if $limitCondition then
        $buildResult
      end
    end
end
return $return
EOF;
    }

    public function buildCondition(string|array $condition, array &$columns): string
    {
        static $builders = [
            'not' => 'buildNotCondition',
            'and' => 'buildAndCondition',
            'or' => 'buildAndCondition',
            'between' => 'buildBetweenCondition',
            'not between' => 'buildBetweenCondition',
            'in' => 'buildInCondition',
            'not in' => 'buildInCondition',
            'like' => 'buildLikeCondition',
            'not like' => 'buildLikeCondition',
            'or like' => 'buildLikeCondition',
            'or not like' => 'buildLikeCondition',
            '>' => 'buildCompareCondition',
            '>=' => 'buildCompareCondition',
            '<' => 'buildCompareCondition',
            '<=' => 'buildCompareCondition',
        ];

        if (!is_array($condition)) {
            throw new NotSupportedException('Where condition must be an array in redis ActiveRecord.');
        }
        if (isset($condition[0])) { // operator format: operator, operand 1, operand 2, ...
            $operator = strtolower($condition[0]);
            if (isset($builders[$operator])) {
                $method = $builders[$operator];
                array_shift($condition);

                return $this->$method($operator, $condition, $columns);
            } else {
                throw new Exception('Found unknown operator in query: ' . $operator);
            }
        } else { // hash format: 'column1' => 'value1', 'column2' => 'value2', ...

            return $this->buildHashCondition($condition, $columns);
        }
    }

    private function buildHashCondition(array $condition, array &$columns): string
    {
        $parts = [];
        foreach ($condition as $column => $value) {
            if (is_array($value)) { // IN condition
                $parts[] = $this->buildInCondition('in', [$column, $value], $columns);
            } else {
                if (is_bool($value)) {
                    $value = (int)$value;
                }
                if ($value === null) {
                    $parts[] = "redis.call('HEXISTS',key .. ':a:' .. pk, " . $this->quoteValue($column) . ")==0";
                } elseif ($value instanceof Expression) {
                    $column = $this->addColumn($column, $columns);
                    $parts[] = "$column==" . $value->expression;
                } else {
                    $column = $this->addColumn($column, $columns);
                    $value = $this->quoteValue($value);
                    $parts[] = "$column==$value";
                }
            }
        }

        return count($parts) === 1 ? $parts[0] : '(' . implode(') and (', $parts) . ')';
    }

    private function buildInCondition(string $operator, array $operands, array &$columns)
    {
        if (!isset($operands[0], $operands[1])) {
            throw new Exception("Operator '$operator' requires two operands.");
        }

        list($column, $values) = $operands;

        $values = (array)$values;

        if (empty($values) || $column === []) {
            return $operator === 'in' ? 'false' : 'true';
        }

        if (is_array($column) && count($column) > 1) {
            return $this->buildCompositeInCondition($operator, $column, $values, $columns);
        } elseif (is_array($column)) {
            $column = reset($column);
        }
        $columnAlias = $this->addColumn($column, $columns);
        $parts = [];
        foreach ($values as $value) {
            if (is_array($value)) {
                $value = isset($value[$column]) ? $value[$column] : null;
            }
            if ($value === null) {
                $parts[] = "redis.call('HEXISTS',key .. ':a:' .. pk, " . $this->quoteValue($column) . ")==0";
            } elseif ($value instanceof Expression) {
                $parts[] = "$columnAlias==" . $value->expression;
            } else {
                $value = $this->quoteValue($value);
                $parts[] = "$columnAlias==$value";
            }
        }
        $operator = $operator === 'in' ? '' : 'not ';

        return "$operator(" . implode(' or ', $parts) . ')';
    }

    protected function buildCompositeInCondition(
        string $operator,
        array $inColumns,
        array $values,
        array &$columns
    ): string {
        $vss = [];
        foreach ($values as $value) {
            $vs = [];
            foreach ($inColumns as $column) {
                if (isset($value[$column])) {
                    $columnAlias = $this->addColumn($column, $columns);
                    $vs[] = "$columnAlias==" . $this->quoteValue($value[$column]);
                } else {
                    $vs[] = "redis.call('HEXISTS',key .. ':a:' .. pk, " . $this->quoteValue($column) . ")==0";
                }
            }
            $vss[] = '(' . implode(' and ', $vs) . ')';
        }
        $operator = $operator === 'in' ? '' : 'not ';

        return "$operator(" . implode(' or ', $vss) . ')';
    }

    private function addColumn(string $column, array &$columns): string
    {
        if (isset($columns[$column])) {
            return $columns[$column];
        }
        $name = 'c' . preg_replace("/[^a-z]+/i", "", $column) . count($columns);

        return $columns[$column] = $name;
    }

    public function buildOne(ActiveQuery $query): string
    {
        $key = $this->getKey($query);
        return $this->build($query, "do return redis.call('HGETALL',$key .. pk) end", 'pks');
    }

    public function buildColumn(ActiveQuery $query, string $column): string
    {
        $key = $this->getKey($query);

        return $this->build(
            $query,
            "n=n+1 pks[n]=redis.call('HGET',$key .. pk," . $this->quoteValue($column) . ")",
            'pks'
        );
    }

    public function buildCount(ActiveQuery $query): string
    {
        return $this->build($query, 'n=n+1', 'n');
    }

    public function buildSum(ActiveQuery $query, string $column): string
    {
        $key = $this->getKey($query);

        return $this->build($query, "n=n+redis.call('HGET',$key .. pk," . $this->quoteValue($column) . ")", 'n');
    }

    public function buildAverage(ActiveQuery $query, string $column): string
    {
        $key = $this->getKey($query);

        return $this->build(
            $query,
            "n=n+1 if v==nil then v=0 end v=v+redis.call('HGET',$key .. pk," . $this->quoteValue($column) . ")",
            'v/n'
        );
    }

    public function buildMin(ActiveQuery $query, string $column): string
    {
        $key = $this->getKey($query);

        return $this->build(
            $query,
            "n=redis.call('HGET',$key .. pk," . $this->quoteValue($column) . ") if v==nil or n<v then v=n end",
            'v'
        );
    }

    public function buildMax(ActiveQuery $query, string $column): string
    {
        $key = $this->getKey($query);

        return $this->build(
            $query,
            "n=redis.call('HGET',$key .. pk," . $this->quoteValue($column) . ") if v==nil or n>v then v=n end",
            'v'
        );
    }

    private function buildNotCondition(string $operator, array $operands, array &$params): string
    {
        if (count($operands) != 1) {
            throw new InvalidArgumentException("Operator '$operator' requires exactly one operand.");
        }

        $operand = reset($operands);
        if (is_array($operand)) {
            $operand = $this->buildCondition($operand, $params);
        }

        return "$operator ($operand)";
    }

    private function buildAndCondition(string $operator, array $operands, array &$columns): string
    {
        $parts = [];
        foreach ($operands as $operand) {
            if (is_array($operand)) {
                $operand = $this->buildCondition($operand, $columns);
            }
            if ($operand !== '') {
                $parts[] = $operand;
            }
        }
        if (!empty($parts)) {
            return '(' . implode(") $operator (", $parts) . ')';
        } else {
            return '';
        }
    }

    private function buildBetweenCondition(string $operator, array $operands, array &$columns): string
    {
        if (!isset($operands[0], $operands[1], $operands[2])) {
            throw new Exception("Operator '$operator' requires three operands.");
        }

        list($column, $value1, $value2) = $operands;

        $value1 = $this->quoteValue($value1);
        $value2 = $this->quoteValue($value2);
        $column = $this->addColumn($column, $columns);

        $condition = "$column >= $value1 and $column <= $value2";
        return $operator === 'not between' ? "not ($condition)" : $condition;
    }

    protected function buildCompareCondition(string $operator, array $operands, array &$columns): string
    {
        if (!isset($operands[0], $operands[1])) {
            throw new Exception("Operator '$operator' requires two operands.");
        }

        list($column, $value) = $operands;

        $column = $this->addColumn($column, $columns);
        if (is_numeric($value)) {
            return "tonumber($column) $operator $value";
        }
        $value = $this->quoteValue($value);
        return "$column $operator $value";
    }

    private function buildLikeCondition(string $operator, array $operands, array &$columns)
    {
        throw new NotSupportedException('LIKE conditions are not suppoerted by redis ActiveRecord.');
    }

    private function getKey(ActiveQuery $query): string
    {
        /* @var $modelClass ActiveRecord */
        $modelClass = create($query->modelClass);
        $db = $query->db;
        $pkey = $db->getCluster() ? '{' . $modelClass->keyPrefix() . '}' : $modelClass->keyPrefix();
        return $this->quoteValue($pkey . ':a:');
    }
}
