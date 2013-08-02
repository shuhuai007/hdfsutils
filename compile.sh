git reset --hard
git fetch origin
git rebase origin/master


ant clean
ant jar

echo "copy jar "
cp build/*.jar script/ObservReportUtil

echo "copy lib"
cp -r lib  script/ObservReportUtil


