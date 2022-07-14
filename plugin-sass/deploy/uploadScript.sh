#部署测试包
echo "打印参数"
profile=$1
fileSuffix="-test"
SCRIPT_ID=196257
if [ "$profile" == "test" ]
then
  echo "测试分支开始部署"
  fileSuffix="-"$profile
elif [ "$profile" == "pre" ]
then
  echo "预发分支开始部署"
  fileSuffix="-"$profile
  SCRIPT_ID=196250
elif [ $profile == "pro" ]
then
  echo "生产分支请走bamboo流水线"
  exit 1
else
  echo "无法识别的参数值"
  exit 1
fi
echo $profile,$fileSuffix,$SCRIPT_ID
cd ../../
pwd
mvn clean -U package -P${profile} -Dmaven.test.skip=true -pl plugin-sass -am
echo "**********开始上传脚本**********"
DESCRIPTION="本地脚本上传"
SCRIPT_NAME="task-plugin-sass${fileSuffix}.zip"
appId=buffalo4-manager.bdp.jd.com
userToken=URM8be7c69bf06773f1d2f5e167322998157908320095480927
appToken=7QNWRWAGLB6JRLFB7PMY3XJISEO37T72SJ7R3RPQ37OZ76WOZMVA

noSpaceDecription=`python -c "import sys; s=sys.argv[1]; tar=s.replace(' ', '-') ; print tar;" "${DESCRIPTION}"`
echo $noSpaceDecription

data="{\"fileId\":${SCRIPT_ID},\"verDescription\":\"$noSpaceDecription\"}"
data=`python -c 'import urllib, sys; print urllib.quote(sys.argv[1])' $data`


#current=`date "+%Y-%m-%d %H:%M:%S"`
#timeStamp=`date -d "$current" +%s`
timeStamp=`date +%s`
echo "timeStamp",$timeStamp
currentTimeStamp=`expr $timeStamp \* 1000`
echo "$appToken$userToken$currentTimeStamp"
sign=`echo -n "$appToken$userToken$currentTimeStamp" | openssl md5 | tr a-z A-Z  | cut -d' ' -f1`
url="http://buffalo.saas-bdp.jd.local/api/v2/buffalo4/script/updateScriptFile?appId=$appId&userToken=$userToken&time=$currentTimeStamp&sign=$sign&data=$data"
echo "请求连接 $url"
rm -rf ${SCRIPT_NAME}
cp ./plugin-sass/target/${SCRIPT_NAME} ${SCRIPT_NAME}
ls -al
response=`curl -X POST -F "file=@${SCRIPT_NAME}" $url`
echo $response

failFlag='"success" : false'
if [[ $response =~ $failFlag ]]
then
exit 1
else
echo "--->success"
fi
echo "脚本上传结束"