import { format as formatSql } from 'sql-formatter';

// DolphinDB 格式化
function formatDolphinDB(code: string): string {
  const lines = code.split('\n');
  let indentLevel = 0;
  const formatted: string[] = [];

  // DolphinDB 关键字（大写）
  const keywords = [
    'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'LIMIT', 'JOIN', 'LEFT JOIN', 'RIGHT JOIN',
    'INNER JOIN', 'ON', 'AS', 'AND', 'OR', 'NOT', 'IN', 'BETWEEN', 'LIKE', 'IS', 'NULL',
    'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TABLE', 'DATABASE',
    'DEF', 'IF', 'ELSE', 'FOR', 'WHILE', 'DO', 'RETURN', 'BREAK', 'CONTINUE',
    'TRY', 'CATCH', 'THROW'
  ];

  for (let line of lines) {
    let trimmed = line.trim();
    if (!trimmed) {
      formatted.push('');
      continue;
    }

    // 减少缩进：else, catch, }
    if (/^(else|catch)\b/.test(trimmed) || trimmed === '}') {
      indentLevel = Math.max(0, indentLevel - 1);
    }

    // 关键字大写转换
    let processedLine = trimmed;
    keywords.forEach(keyword => {
      const regex = new RegExp(`\\b${keyword}\\b`, 'gi');
      processedLine = processedLine.replace(regex, keyword);
    });

    // 添加缩进
    formatted.push('    '.repeat(indentLevel) + processedLine);

    // 增加缩进：{ 或 def/if/else/for/while/do/try 后面跟 {
    if (trimmed.endsWith('{')) {
      indentLevel++;
    } else if (/^(def|if|else|for|while|do|try)\b/.test(trimmed) && !trimmed.includes('{')) {
      indentLevel++;
    }

    // 减少缩进：}
    if (trimmed === '}') {
      // 已经在上面处理过了
    }
  }

  return formatted.join('\n');
}

// 简单的 Python 缩进格式化
function formatPython(code: string): string {
  const lines = code.split('\n');
  let indentLevel = 0;
  const formatted: string[] = [];

  for (let line of lines) {
    const trimmed = line.trim();
    if (!trimmed) {
      formatted.push('');
      continue;
    }

    // 减少缩进：else, elif, except, finally, elif
    if (/^(else|elif|except|finally):/.test(trimmed)) {
      indentLevel = Math.max(0, indentLevel - 1);
    }

    // 添加缩进
    formatted.push('    '.repeat(indentLevel) + trimmed);

    // 增加缩进：以冒号结尾的行（函数、类、if、for、while 等）
    if (trimmed.endsWith(':')) {
      indentLevel++;
    }

    // 减少缩进：return, break, continue, pass, raise 后面如果没有更多代码
    if (/^(return|break|continue|pass|raise)(\s|$)/.test(trimmed)) {
      // 不立即减少，等下一行判断
    }
  }

  return formatted.join('\n');
}

export async function formatCode(code: string, language: string): Promise<string> {
  try {
    switch (language) {
      case 'python':
        return formatPython(code);

      case 'sql':
      case 'dolphindb':
        return formatDolphinDB(code);

      case 'json':
        return JSON.stringify(JSON.parse(code), null, 2);

      default:
        return code;
    }
  } catch (error: any) {
    console.error('Format error:', error);
    throw new Error(`格式化失败: ${error.message}`);
  }
}
