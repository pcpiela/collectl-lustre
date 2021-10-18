# copyright, 2014 Terascala, Inc. All rights reserved
#
# lustreSingleton may be copied only under the terms of either the 
# Artistic License or the GNU General Public License
package LustreSingleton;


sub new {
	my $class = shift;
	$singleton = $singleton || bless {}, $class;
}

sub setDebug {
	$this = shift;
	$this->{_debug} = shift;
}

sub getVersion {
	$this = shift;
	# the null if block, a self-documenting
	if( $this->{_version} )
	{
		# version already retrieved, do nothing
		if($this->{_debug}){ print "version already retrieved, do nothing\n"; }
	}
	else
	{
		my $version_filename = '/proc/fs/lustre/version';
		if (-e $version_filename) {
			if($this->{_debug}){ print "retrieving version\n"; }
			open(FF,"<$version_filename");
			my $version_line = <FF>;
			close(FF);
			chomp($version_line);
			my @version = split(" ",$version_line);
			$this->{_version} = $version[1];	
		}
		else {
			my $version_filename = '/sys/fs/lustre/version';
			if (-e $version_filename) {
				if($this->{_debug}){ print "retrieving version\n"; }
				open(FF,"<$version_filename");
				my $version_line = <FF>;
				close(FF);
				chomp($version_line);
				$this->{_version} = $version_line;
			}
		}
	}
	return $this->{_version};
}


sub getTarget {
	$this = shift;

	# lusost.writes.lmp-OST0002
	# lustost.diskIoSizeRead.lmp-OST0000.0004k
	my $metric = shift;

	my $fsname = $this->{_fsname};

	if( $metric =~ m/^lusost/ || $metric =~ m/^lustost/ )
	{
		my @bits = split(/\./,$metric);
		my $size = scalar(@bits);
		my $target = $bits[2]; 

		$target =~ s/^\Q$fsname\E-//;
		return $target;
	}

	return '';
} 
	
sub determineLustreParameters {
	$this = shift;
	$names = shift;
	@names = @$names;
	my $server_type = 'MDS';
	my $fsname = '';
	if( $this->{_isLustreChecked} )
	{
		if($this->{_debug}){ print "lustre parameters already determined, do nothing\n"; }
	}
	else
	{
		$this->{_isLustreChecked} = 1;
		foreach my $name (@names)
		{
			if( $name =~ m/^lustost\.[^\.]+\.([^\.]+)\..*$/ ) # lustost.<some non-dots>.fsname-MDT0000.<some stuff>
			{
				$server_type = 'OSS';
				$fsname = $1;
				$fsname =~ s/-.*$//;
				last;
			 }

			if( $name =~ m/^lusmds/ )
			{
				$server_type = 'MDS';
				$fsname = $this->getFSNameFromProc();						
				last;
			}

		}
		$this->{_server_type} = $server_type;
		$this->{_fsname} = $fsname;
	}
}

sub getServerType {
	$this = shift;
	return $this->{_server_type};
}	

sub getFSName {
	$this = shift;
	return $this->{_fsname};
}	

sub getFSNameFromProc {
	$this = shift;
	if( $this->{_fsname} )
	{
		# fsname already retrieved, do nothing
		if($this->{_debug}){ print "fsname already retrieved, do nothing\n"; }
	}
	else
	{
		$lustre_version = $this->getVersion();
		my $mdtDir = '/proc/fs/lustre/';
		$mdtDir .= ($lustre_version ge '2.1.1') ? 'mdt' : 'mds';
 
		opendir(DIR,$mdtDir);
		while( my $file = readdir(DIR))
		{ 
			if( -d	"$mdtDir/$file" && $file ne '.' && $file ne '..' && $file =~ m/-/ )
			{ 
				$fsname = $file;
				$fsname =~ s/-.*$//;
			}
		}
		closedir(DIR);
		$this->{_fsname} = $fsname;
	}

	return $this->{_fsname};
}


1;
