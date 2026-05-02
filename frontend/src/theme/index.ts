import { theme } from 'antd';
import type { ThemeConfig } from 'antd';

export const darkTheme: ThemeConfig = {
  algorithm: theme.darkAlgorithm,
  token: {
    colorPrimary: '#1677FF',
    colorBgBase: '#0D1117',
    colorBgContainer: '#161B22',
    colorBgElevated: '#1C2128',
    colorBorder: '#30363D',
    colorText: '#E6EDF3',
    colorTextSecondary: '#8B949E',
    colorSuccess: '#3FB950',
    colorError: '#F85149',
    colorWarning: '#D29922',
    borderRadius: 4,
    fontSize: 12,
    fontFamily: "'SF Pro Display', -apple-system, BlinkMacSystemFont, 'PingFang SC', 'Microsoft YaHei', sans-serif",
    controlHeight: 28, // 默认控件高度标准：28px (middle)，所有组件不指定 size 时走此默认值
    paddingXS: 6,
    paddingSM: 8,
    padding: 12,
    paddingMD: 16,
    paddingLG: 20,
    marginXS: 6,
    marginSM: 8,
    margin: 12,
    marginMD: 16,
    marginLG: 20,
  },
  components: {
    Menu: { darkItemBg: '#080C15', darkSubMenuItemBg: '#0D1117', itemHeight: 36 },
    Table: { headerBg: '#161B22', rowHoverBg: '#1C2128', cellPaddingBlock: 6, cellPaddingInline: 10 },
    Drawer: { colorBgElevated: '#1C2128' },
    Tabs: { cardPadding: '6px 12px' },
    Card: { paddingLG: 12 },
  },
};

export const lightTheme: ThemeConfig = {
  algorithm: theme.defaultAlgorithm,
  token: {
    colorPrimary: '#1677FF',
    colorBgBase: '#F6F8FA',
    colorBgContainer: '#FFFFFF',
    colorBorder: '#D0D7DE',
    colorText: '#1F2328',
    colorTextSecondary: '#656D76',
    borderRadius: 4,
    fontSize: 12,
    fontFamily: "'SF Pro Display', -apple-system, BlinkMacSystemFont, 'PingFang SC', 'Microsoft YaHei', sans-serif",
    controlHeight: 28, // 默认控件高度标准：28px (middle)，所有组件不指定 size 时走此默认值
    paddingXS: 6,
    paddingSM: 8,
    padding: 12,
    paddingMD: 16,
    paddingLG: 20,
    marginXS: 6,
    marginSM: 8,
    margin: 12,
    marginMD: 16,
    marginLG: 20,
  },
  components: {
    Menu: { itemColor: '#1F2328', itemSelectedColor: '#1677FF', itemHoverColor: '#1677FF', itemSelectedBg: 'rgba(22,119,255,0.08)' },
    Table: { cellPaddingBlock: 6, cellPaddingInline: 10 },
    Tabs: { cardPadding: '6px 12px' },
    Card: { paddingLG: 12 },
  },
};
