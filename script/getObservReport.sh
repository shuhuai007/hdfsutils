#!/usr/bin/env bash

if [ $# -le 0 ]
then
  echo 'you must pass the necessary parameter using "--hdfsHostUrl hdfs://sm98:9000  --observReportHDFSDir /user/hadoop/mifc_etl/output_observer_report  --observReportBEDir  /home/hadoop/zhoujie/project/hdfsutils"'
  exit;
fi

while [ -n "$1" ]; do
  case "$1" in
    --hdfsHostUrl)
        shift
        hdfsHostUrl=$1
        shift
        ;;
    --observReportHDFSDir)
        shift
        observReportHDFSDir=$1
        shift
        ;;
    --observReportBEDir)
        shift
        observReportBEDir=$1
        shift
        ;;
    *)
        break;
        ;;
  esac
done

echo "$hdfsHostUrl"
echo "$observReportHDFSDir"
echo "$observReportBEDir"
#exit

#java -cp ObservReportUtil/hdfsutils.jar com.allyes.hdfsutils.GetObservReportJSON --hdfsHostUrl=hdfs://sm98:9000  --observReportHDFSDir=/user/hadoop/mifc_etl/output_observer_report  --observReportBEDir=/home/hadoop/zhoujie/project/hdfsutils
java -cp ObservReportUtil/hdfsutils.jar com.allyes.hdfsutils.GetObservReportJSON --hdfsHostUrl=${hdfsHostUrl} --observReportHDFSDir=${observReportHDFSDir} --observReportBEDir=${observReportBEDir}
