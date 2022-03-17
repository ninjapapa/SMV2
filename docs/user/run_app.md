# Running SMV Application

While an SMV application can be built and run using the standard "spark-submit" command

### Synopsis
```shell
$ cd project_dir
$ spark-submit src/main/python/appdriver.py [spark-submit-options] -- [smv-options] [appdriver-options]
```

**Note:**  The above command should be run from your project's top level directory, 
otherwise need to use `--smv-app-dir` to specify project dir.

### Options
<br>
<table>

<tr>
<th colspan="3">Options</th>
</tr>

<tr>
<th>Option</th>
<th>Default</th>
<th>Description</th>
</tr>

<tr>
<td>--smv-app-dir</td>
<td>./</td>
<td>allow user to specify project dir</td>
</tr>

<tr>
<td>--smv-props</td>
<td>None</td>
<td>allow user to specify a set of config properties on the command line.
<br>
<code>$ ... --smv-props "smv.stages=s1"</code>
<br>
See <a href="app_config.md">Application Configuration</a> for details.
</td>
</tr>

<tr>
<td>--force-run-all</td>
<td>off</td>
<td>Remove <b>ALL</b> files in output directory that <b>are</b> the  current version of the outputs in the app, forcing all specified modules to run even if they have been run recently.
</tr>

<tr>
<td>--publish version</td>
<td>off</td>
<td>publish the specified modules to the given version</td>
</tr>

<tr>
<td>--dry-run </td>
<td>off</td>
<td>Find which modules do not have persisted data, among the modules that need to be run. When specified, no modules are actually executed.
</td>
</tr>

<tr>
<th colspan="3">What To Run/Publish
<br>
One of the options below must be specified.
</th>
</tr>

<tr>
<th colspan="2">Option</th>
<th>Description</th>
</tr>

<tr>
<td colspan="2">--run-module mod1 [mod2 ...] / &#8209;m</td>
<td>Run the provided list of modules directly (even if they are not marked as SmvOutput module)
</td>
</tr>

<tr>
<td colspan="2">--run-stage stage1 [stage2 ...] / &#8209;s</td>
<td>Run all output modules in given stages.  The stage name can either be the base name of the stage or the FQN.
</td>
</tr>

<tr>
<td colspan="2">--run-app, -r </td>
<td>Run all output modules in all configured stages in current app.
</td>
</tr>

<tr>
<th colspan="3">Data Directories Config Override</th>
</tr>

<tr>
<th>Option</th>
<th>Config<br>Override</th>
<th>Description</th>
</tr>

<tr>
<td>&#8209;&#8209;data&#8209;dir</td>
<td>smv.dataDir</td>
<td>option to override default location of top level data directory</td>
</tr>

</table>

### Examples
Run modules `M1` and `M2` and all its dependencies.  Note the use of the module FQN.
```shell
$ spark-submit src/main/python/appdriver.py -- -m com.mycom.myproj.stage1.M1 com.mycom.myproj.stage1.M2
```

Publish the output modules in stage "s1" as version "xyz".  The modules will be output to `data/publish/xyz` dir.
```shell
$ spark-submit src/main/python/appdriver.py -- --publish xyz  -s s1
```

Provide spark specific arguments
```shell
$ spark-submit --executor-memory=2G --driver-memory=1G --master yarn-client src/main/python/appdriver.py -- -r
```
****
