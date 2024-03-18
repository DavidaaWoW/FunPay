<?php

namespace FpDbTest\src;

use Exception;
use mysqli;

class SkipValue{}

class Database implements DatabaseInterface
{

    private mysqli $mysqli;
    private $spec_types;

	/**
	 * @param mysqli     $mysqli
	 * @param array|null $spec_types
	 */
	public function __construct(mysqli $mysqli, array $spec_types = null)
    {
        $this->mysqli = $mysqli;
        $this->spec_types = [
            'integer' => '?d',
            'float' => '?f',
            'array' => '?a',
            'ids' => '?#'
        ];
        // Дополняем массив спецификаторов, при необходимости
        if($spec_types)
            foreach ($spec_types as $key=>$val)
                $spec_types[$key] = $val;
    }

	/**
	 * @param $haystack
	 * @param $needle
	 * @return array
	 */
	private function findAllStrPos($haystack, $needle) {
        $offset = 0;
        $allpos = array();
        while (($pos = strpos($haystack, $needle, $offset)) !== FALSE) {
            $offset = $pos + 1;
            $allpos[] = $pos;
        }
        return $allpos;
    }

	/**
	 * @param $query
	 * @param $spec_pos
	 * @param $left_braces
	 * @param $right_braces
	 * @return array|string|string[]
	 */
	private function dropOptionalText($query, $spec_pos, $left_braces, $right_braces){
        $left = $left_braces[0];
        $right = $right_braces[count($right_braces)-1];
        // Приближением находим ближайшие условные блоки
        foreach ($left_braces as $lb){
            if($lb < $spec_pos){
                $left = $lb;
            }
        }
        foreach ($right_braces as $rb){
            if($rb > $spec_pos){
                $right = $rb;
            }
        }
        // Удаляем текст между опциональным блоком
        return substr_replace($query, '', $left, $right);
    }

	/**
	 * @param $arg
	 * @param $spec_type
	 * @param $slashes
	 * @param $apostrophe
	 * @return mixed|string|null
	 * @throws Exception
	 */
	private function castType($arg, $spec_type, $slashes = false, $apostrophe = false){
        $type = gettype($arg);
        if($type == 'string')
        $arg = addslashes($arg);
        if($type != $spec_type) {
            // Пришёл null - кладём null
            if($arg == null) $arg = null;
            // Пытаемся кастовать в соответствии с нужным спец.
            else {
                $res = settype($arg, $spec_type);
                if (!$res)
                    throw new Exception("Аргумент $arg не совпадает с переданным типом $spec_type и не может быть преобразован");
            }
        }
        // Если обрабатываем масив, то заменяем значения
        if($slashes && gettype($arg) == 'string') $arg = "\'$arg\'";
        if($slashes && gettype($arg) == 'NULL') $arg = 'NULL';
        // Добавляем кавычки, там где нужно
        if($apostrophe) $arg = "`$arg`";
        return $arg;
    }

	/**
	 * @param string $query
	 * @param array  $args
	 * @return string
	 * @throws Exception
	 */
	public function buildQuery(string $query, array $args = []): string
    {
        $spec_count = substr_count($query, '?');
        $args_count = count($args);
        // Проверка на совпадение количества спец.
        if($spec_count !== $args_count)
            throw new Exception("Количество спецификаторов $spec_count не совпадает с количеством аргументов $args_count");
        preg_match_all('/\?./u', $query, $matches);
        $str_copy = $query;
        $replacement = [];

        foreach ($matches[0] as $i=>$match){
            if($args[$i] instanceof \FpDbTest\SkipValue ){
                // Определяем текущее положение
                $spec_pos = strpos($query, $match);
                // Определяем все позиции
                $left_braces = $this->findAllStrPos($query, '{');
                $right_braces = $this->findAllStrPos($query, '}');

                $query = $this->dropOptionalText($query, $spec_pos, $left_braces, $right_braces);

                continue;
            }
            // Если передан спецификатор
            if(in_array($match, $this->spec_types)){
                $spec_type = array_search($match, $this->spec_types);
                // Обрабатываем идентификаторы
                if($spec_type == 'ids'){
                    // Не массив -> обрабатываем как строку
                    if(!is_array($args[$i]))
                        array_push($replacement, $this->castType($args[$i], 'string', apostrophe: true));
                    // Массив -> обрабатываем как обычный массив
                    else {
                        $arr_string = '';
                        foreach ($args[$i] as $key=>$value) {
                            $arr_string .= $this->castType($value, 'string', apostrophe: true);
                            if($key != count($args[$i]) - 1) $arr_string.=', ';
                        }
                        array_push($replacement, $arr_string);
                    }
                }
                // Обрабатываем массивы
                elseif ($spec_type == 'array'){
                    $arr_string = '';
                    // Внешний итератор для корректного расставления запятых
                    $j = 0;
                    foreach ($args[$i] as $key=>$value){
                        // Если массив ассоциатевный, обрабатываем также ключ
                        if(!is_numeric($key)) {
                            $arr_string .= $this->castType($key, gettype($key), apostrophe: true);
                            $arr_string .= ' = ';
                        }
                        // Обрабатываем значение
                        $arr_string .= $this->castType($value, gettype($value), true);
                        if($j != count($args[$i]) - 1) $arr_string.=', ';
                        $j++;
                    }
                    array_push($replacement, $arr_string);
                }
                // Обрабатываем остальные идентификаторы
                else {
                    array_push($replacement, $this->castType($args[$i], $spec_type));
                }
            }
            // Если спецификатор пуст
            elseif ($match[1] == ' '){
                $type = gettype($args[$i]);
                switch ($type){
                    case 'float':
                    case 'integer':
                        array_push($replacement, $args[$i]);
                        break;
                    case 'string':
                        array_push($replacement,  $this->castType($args[$i], 'string', true));
                        break;
                    case 'boolean':
                        array_push($replacement, !!$args[$i]);
                        break;
                    case 'NULL':
                        array_push($replacement, "NULL");
                        break;
                    default:
                        throw new Exception("Аргумент $args[$i] имеет не поддерживаемый тип $type");
                }
            }
            // Неизвестный спецификатор
            else{
                throw new Exception("Спецификатор $match не найден в списке");
            }
        }
        $i = 0;
        // Тут начинается замена
        $query = preg_replace_callback('/\?./u', function ($matches) use ($replacement, &$i) {

            $ret = $replacement[$i];
            $i++;
            // Если не уникальный спецификатор нужно добавлять доп. пробел под регулярку
            if($matches[0][1] == ' ') $ret .= ' ';
            return $ret;

        }, $query);
        // Убираем кавычки
        $query = str_replace(['{', '}'], '', $query);
        var_dump($query);
        echo '<br>';
//        var_dump($replacement);
//        echo '<br>';
//        echo '<br>';

        return $query;
//        throw new Exception();
    }

	/**
	 * @return \FpDbTest\SkipValue
	 */
	public function skip()
    {
        return new \FpDbTest\SkipValue();
    }
}
