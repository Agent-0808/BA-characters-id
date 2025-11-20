# BA-characters-internal-id

整理角色对应的文件ID，方便解包时确定对应文件。

可以从Release中下载最新版本的CSV文件，避免多次请求API。

### 示例

Steam版的文件储存路径为`BlueArchive\BlueArchive_Data\StreamingAssets\PUB\Resource\GameData\Windows\`目录

- 查表得，`霞沢 ミユ`的文件ID为 `CH0145`，则可以获取其对应文件为：
  - 人物模型：`assets-_mx-characters-ch0145-_mxdependency-2024-11-18_assets_all_*.bundle`
  - 人物立绘：`assets-_mx-spinecharacters-ch0145_spr-_mxdependency-2024-11-18_assets_all_*.bundle`
  - 回忆大厅：`assets-_mx-spinelobbies-ch0145_home-_mxdependency-2024-11-18_assets_all_*.bundle`
  - .....

手机版文件名同理

### 致谢

- 信息来自 [基沃托斯古书馆](https://kivo.wiki)，感谢

- API用法参考：[说明](https://github.com/Agent-0808/bluearchive-api-kivowiki)（[原仓库](https://github.com/Dale233/bluearchive-api-kivowiki)）

- 代码由AI编写