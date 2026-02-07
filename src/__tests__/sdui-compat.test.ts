import { describe, expect, test } from 'bun:test';

import {
  checkSduiCompat,
  parseSduiVersion,
} from '../services/sdui-compat';

describe('sdui compat', () => {
  test('同版本兼容并协商到插件版本', () => {
    const result = checkSduiCompat('1.0', '1.0');

    expect(result).toEqual({
      compatible: true,
      negotiated: '1.0',
    });
  });

  test('core 次版本更高时兼容并协商到插件版本', () => {
    const result = checkSduiCompat('1.3', '1.1');

    expect(result).toEqual({
      compatible: true,
      negotiated: '1.1',
    });
  });

  test('major 不匹配时回退到 HIDE', () => {
    const result = checkSduiCompat('2.0', '1.5');

    expect(result).toEqual({
      compatible: false,
      reason: 'MAJOR_MISMATCH',
      fallback: 'HIDE',
    });
  });

  test('core minor 不足时回退到 BASIC_FALLBACK', () => {
    const result = checkSduiCompat('1.0', '1.3');

    expect(result).toEqual({
      compatible: false,
      reason: 'MINOR_TOO_LOW',
      fallback: 'BASIC_FALLBACK',
    });
  });

  test('插件未声明版本时默认按 1.0 判定', () => {
    const result = checkSduiCompat('1.2');

    expect(result).toEqual({
      compatible: true,
      negotiated: '1.0',
    });
  });

  test('非法版本格式会返回 null', () => {
    expect(parseSduiVersion('abc')).toBeNull();
    expect(parseSduiVersion('')).toBeNull();
    expect(parseSduiVersion('1')).toBeNull();
  });

  test('边界值 1.0 对 1.0 精确匹配兼容', () => {
    const result = checkSduiCompat('1.0', '1.0');

    expect(result).toEqual({
      compatible: true,
      negotiated: '1.0',
    });
  });

  test('parseSduiVersion 可以解析合法版本', () => {
    expect(parseSduiVersion('12.34')).toEqual({ major: 12, minor: 34 });
  });
});
