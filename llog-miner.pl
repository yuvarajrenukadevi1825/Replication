#!/usr/bin/perl
#
# Parse connection rate from Oracle listener log into CSV format
# Neil Johnson
# oraganism.wordpress.com
#
# Modification history
# 12 Apr 2012: Initial version
# 18 Apr 2012: Switched foreach to while to avoid memory errors on big files
# 02 May 2012: Added getopt code.
#              - Filter now -filter|-f and NOT $2.
#              - Can use "-" for STDIN.
#              - Added -[no]excel|-e
#              - Added -[no]host
#
# TODO
# Add code to filter on target when multiple DBs serviced by single listener
# Add code to fill in missing minutes
# Consider allowing multiple listener log parameters?
#

use strict;
use warnings;
use File::Basename;
use Getopt::Long;

# some constants
my %months = ( "JAN" => "01", "FEB" => "02", "MAR" => "03", "APR" => "04"
            ,  "MAY" => "05", "JUN" => "06", "JUL" => "07", "AUG" => "08"
            ,  "SEP" => "09", "OCT" => "10", "NOV" => "11", "DEC" => "12");

# some declarations
my %progNames;
my %logHash;
my ($progPos, $hostPos, $prog, $host);
my ($time, $timeAsNumber, $splitTime, $lastTime, $lastTimeAsNumber);
$lastTimeAsNumber=0;
my ($i, $yyyy, $mon, $mm, $dd,$hh, $mi, $garbage);
my $progId;
my $logName;
my $logLine;

# parse command line
#
#
my $debug = 0;
my $stdin = 0;
my $includeHost = 1;
my $help = 0;
my $excelOutput = 0;
my $filter = '.';
if(! GetOptions ('debug' => \$debug
          , 'excel!' => \$excelOutput
          , 'filter:s' => \$filter
          , 'help|?' => \$help
          , 'host!' => \$includeHost
          , '' => \$stdin))
{
    $help = 1
};

if($debug) {
    print "debug=$debug\n";
    print "excelOutput=$excelOutput\n";
    print "filter=$filter\n";
    print "includeHost=$includeHost\n";
    print "stdin=$stdin\n";
    print "#ARGV=$#ARGV\n";
}

if (($#ARGV < 0 && ! $stdin) || $help == 1) {
    print "
    Usage: $0 [-help] [-filter <string>] [-[no]excel] [-[no]host] <listener_log_path|->
         : -excel|noexcel       : Output dates in Excel friendly format (default is noexcel)
         : -help                : Print this help
         : -host|nohost         : Include host on program (default is to include)
         : -filter <string>     : A regexp to match beginning of log line. e.g. \"23-DEC-2012 0[12]\" or \"..-MAR-2013\"
         : listener_log_path|-  : Input either a valid listener log name or \"-\" to read STDIN

    Examples:
         # Process listener.log looking for entries on the 21st and 22nd of Sept and format dates for Excel
         C:\\>perl llog-miner.pl -filter \"2[12]-SEP-2011\" -excel listener.log

	 # Process two listener log files looking for entries on Jan 3rd between 20:00 and 21:59 and exclude the hostname
         \$ cat listener.log listener_db.log | llog-miner.pl -filter \"03-JAN-2012 2[01]\" -nohost -
    ";
    exit;
}

if($stdin) {
    $logName = "-";
} else {
    $logName = $ARGV[0];
}

if($debug) { print "logName=$logName\n"; }

open(LOGSTREAM, "<$logName") or die "Could not open '$logName' $!\n";

while(<LOGSTREAM>) {
    $logLine = $_;

    next if (! ($logLine =~ m/^$filter/ && $logLine =~ m/establish/));

    $progPos=index($logLine,"PROGRAM")+8;
    # first HOST should be of client
    $hostPos=index($logLine,"HOST")+5;

    # pull out key fields
    # TIMESTAMP * CONNECT DATA [* PROTOCOL INFO] * EVENT [* SID] * RETURN CODE

    ($splitTime = substr($logLine,0,17)) =~ s/[ :]/-/g;
    ($dd, $mon, $yyyy, $hh, $mi, $garbage) = split('-',$splitTime);
    $mm = $months{$mon};
    $time = "$yyyy-$mm-$dd $hh:$mi";
    $timeAsNumber = ($dd*60*24) + ($hh*60) + $mi;

    $prog = lc(substr($logLine,$progPos,index($logLine,")",$progPos)-$progPos));
    $host = lc(substr($logLine,$hostPos,index($logLine,")",$hostPos)-$hostPos));

    # remove any commas
    $prog =~ s/\,/\_/g;
    $host =~ s/\,/\_/g;

    if($debug) {
        print "time:{".$time."} {$timeAsNumber} {$lastTimeAsNumber}\n";
        print "prog:{".$prog."}\n";
        print "host:{".$host."}\n";
    }

    if($prog eq "" && $host eq "__jdbc__") {
	$prog = $host;
	$host = "";
    } elsif($prog eq "") {
	$prog = "unknown";
    } else {
	# remove the path from the program
	if($prog =~ m/^[c-z]:/) {
        	fileparse_set_fstype("MSWin32");
        } else {
        	fileparse_set_fstype("Unix");
        }
        ($prog,$garbage) = fileparse($prog);
    }

    if($includeHost && $host ne "") {
	# tag the hostname on to the end of the program
	# probably need a switch to make this optional
        $progId=$prog."@".$host;
    } else {
        $progId=$prog;
    }

    if(! exists($progNames{$progId})) { $progNames{$progId}=1; }

    if(! exists($logHash{$time}) || ! exists($logHash{$time}{$progId})) {
        $logHash{$time}{$progId} = 1;
    } else {
        $logHash{$time}{$progId}++;
    }

    $lastTimeAsNumber = $timeAsNumber;
    $lastTime = $splitTime;
}

close(LOGSTREAM);

#
# print header record
#
print "Timestamp (YYYY-MM-DD HH24:MI)";
for my $progId (sort keys %progNames) { 
	print ",".$progId;
}
print "\n";

#
# print log data
#
for my $logTime (sort keys %logHash) {
    # ugly messing about to stop Excel messing with dates
    if($excelOutput == 1) {
        print "=\" $logTime\"";
    } else {
        print "$logTime";
    }

    for my $progId (sort keys %progNames) { 
        print ",";
	if(exists($logHash{$logTime}{$progId})) {
            print $logHash{$logTime}{$progId}
	} else {
            print "0";
        }
    }

    print "\n";
}