<?php

namespace rabbit\db\redis;

use rabbit\db\Exception;
use rabbit\db\Expression;
use rabbit\exception\InvalidArgumentException;
use rabbit\exception\NotSupportedException;

/**
 * Class LuaScriptBuilder
 * @package rabbit\db\redis
 */
class LuaScriptBuilder
{
    /**
     * Builds a Lua script for finding a list of records
     * @param ActiveQuery $query the query used to build the script
     * @return string
     */
    public function buildAll(ActiveQuery $query): string
    {
        /* @var $modelClass ActiveRecord */
        $modelClass = $query->modelClass;
        $key = $this->quoteValue($modelClass::keyPrefix() . ':a:');

        return $this->build($query, "n=n+1 pks[n]=redis.call('HGETALL',$key .. pk)", 'pks');
    }

    /**
     * Quotes a string value for use in a query.
     * Note that if the parameter is not a string or int, it will be returned without change.
     * @param string $str string to be quoted
     * @return string the properly quoted string
     */
    private function quoteValue(string $str): string
    {
        if (!is_string($str) && !is_int($str)) {
            return $str;
        }

        return "'" . addcslashes($str, "\000\n\r\\\032\047") . "'";
    }

    /**
     * @param ActiveQuery $query the query used to build the script
     * @param string $buildResult the lua script for building the result
     * @param string $return the lua variable that should be returned
     * @return string
     * @throws NotSupportedException when query contains unsupported order by condition
     */
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

        /* @var $modelClass ActiveRecord */
        $modelClass = $query->modelClass;
        $key = $this->quoteValue($modelClass::keyPrefix());
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

    /**
     * Parses the condition specification and generates the corresponding Lua expression.
     * @param string|array $condition the condition specification. Please refer to [[ActiveQuery::where()]]
     * on how to specify a condition.
     * @param array $columns the list of columns and aliases to be used
     * @return string the generated SQL expression
     * @throws \rabbit\db\Exception if the condition is in bad format
     * @throws \rabbit\exception\NotSupportedException if the condition is not an array
     */
    public function buildCondition($condition, array &$columns): string
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

    /**
     * @param array $condition
     * @param array $columns
     * @return string
     * @throws Exception
     */
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

    /**
     * @param string $operator
     * @param array $operands
     * @param array $columns
     * @return string
     * @throws Exception
     */
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

    /**
     * @param string $operator
     * @param array $inColumns
     * @param array $values
     * @param array $columns
     * @return string
     */
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

    /**
     * Adds a column to the list of columns to retrieve and creates an alias
     * @param string $column the column name to add
     * @param array $columns list of columns given by reference
     * @return string the alias generated for the column name
     */
    private function addColumn(string $column, array &$columns): string
    {
        if (isset($columns[$column])) {
            return $columns[$column];
        }
        $name = 'c' . preg_replace("/[^a-z]+/i", "", $column) . count($columns);

        return $columns[$column] = $name;
    }

    /**
     * Builds a Lua script for finding one record
     * @param ActiveQuery $query the query used to build the script
     * @return string
     */
    public function buildOne(ActiveQuery $query): string
    {
        /* @var $modelClass ActiveRecord */
        $modelClass = $query->modelClass;
        $key = $this->quoteValue($modelClass::keyPrefix() . ':a:');

        return $this->build($query, "do return redis.call('HGETALL',$key .. pk) end", 'pks');
    }

    /**
     * Builds a Lua script for finding a column
     * @param ActiveQuery $query the query used to build the script
     * @param string $column name of the column
     * @return string
     */
    public function buildColumn(ActiveQuery $query, string $column): string
    {
        // TODO add support for indexBy
        /* @var $modelClass ActiveRecord */
        $modelClass = $query->modelClass;
        $key = $this->quoteValue($modelClass::keyPrefix() . ':a:');

        return $this->build($query, "n=n+1 pks[n]=redis.call('HGET',$key .. pk," . $this->quoteValue($column) . ")",
            'pks');
    }

    /**
     * Builds a Lua script for getting count of records
     * @param ActiveQuery $query the query used to build the script
     * @return string
     */
    public function buildCount(ActiveQuery $query): string
    {
        return $this->build($query, 'n=n+1', 'n');
    }

    /**
     * Builds a Lua script for finding the sum of a column
     * @param ActiveQuery $query the query used to build the script
     * @param string $column name of the column
     * @return string
     */
    public function buildSum(ActiveQuery $query, string $column): string
    {
        /* @var $modelClass ActiveRecord */
        $modelClass = $query->modelClass;
        $key = $this->quoteValue($modelClass::keyPrefix() . ':a:');

        return $this->build($query, "n=n+redis.call('HGET',$key .. pk," . $this->quoteValue($column) . ")", 'n');
    }

    /**
     * Builds a Lua script for finding the average of a column
     * @param ActiveQuery $query the query used to build the script
     * @param string $column name of the column
     * @return string
     */
    public function buildAverage(ActiveQuery $query, string $column): string
    {
        /* @var $modelClass ActiveRecord */
        $modelClass = $query->modelClass;
        $key = $this->quoteValue($modelClass::keyPrefix() . ':a:');

        return $this->build($query,
            "n=n+1 if v==nil then v=0 end v=v+redis.call('HGET',$key .. pk," . $this->quoteValue($column) . ")", 'v/n');
    }

    /**
     * Builds a Lua script for finding the min value of a column
     * @param ActiveQuery $query the query used to build the script
     * @param string $column name of the column
     * @return string
     */
    public function buildMin(ActiveQuery $query, string $column): string
    {
        /* @var $modelClass ActiveRecord */
        $modelClass = $query->modelClass;
        $key = $this->quoteValue($modelClass::keyPrefix() . ':a:');

        return $this->build($query,
            "n=redis.call('HGET',$key .. pk," . $this->quoteValue($column) . ") if v==nil or n<v then v=n end", 'v');
    }

    /**
     * Builds a Lua script for finding the max value of a column
     * @param ActiveQuery $query the query used to build the script
     * @param string $column name of the column
     * @return string
     */
    public function buildMax(ActiveQuery $query, string $column): string
    {
        /* @var $modelClass ActiveRecord */
        $modelClass = $query->modelClass;
        $key = $this->quoteValue($modelClass::keyPrefix() . ':a:');

        return $this->build($query,
            "n=redis.call('HGET',$key .. pk," . $this->quoteValue($column) . ") if v==nil or n>v then v=n end", 'v');
    }

    /**
     * @param string $operator
     * @param array $operands
     * @param array $params
     * @return string
     * @throws Exception
     * @throws NotSupportedException
     */
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

    /**
     * @param string $operator
     * @param array $operands
     * @param array $columns
     * @return string
     * @throws Exception
     * @throws NotSupportedException
     */
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

    /**
     * @param string $operator
     * @param array $operands
     * @param array $columns
     * @return string
     * @throws Exception
     */
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

    /**
     * @param string $operator
     * @param array $operands
     * @param array $columns
     * @throws NotSupportedException
     */
    private function buildLikeCondition(string $operator, array $operands, array &$columns)
    {
        throw new NotSupportedException('LIKE conditions are not suppoerted by redis ActiveRecord.');
    }
}
