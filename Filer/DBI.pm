package Oak::Filer::DBI;

use base qw(Oak::Filer);

use DBI;

use strict;

=head1 NAME

Oak::Filer::DBI - Filer to save/load data into/from DBI tables

=head1 SYNOPSIS

  require Oak::Filer::DBI;

  my $filer = new Oak::Filer::DBI
   (
    dbdriver => "mysql",	# mandatory, any supported by DBI
    database => "mydatabase",	# mandatory
    hostname => "hostname",	# mandatory
    table => "tablename",	# mandatory to enable load and store.
				#   table to work in selects and updates
    where => {primary => value},# this option must be passed to
				#   enable load and store functions.
				#   name and value of the keys to where sql clause
    username => "dbusername",	# optional
    password => "userpasswd",	# optional
    options => { DBI OPTIONS },	# optional. A hash reference to DBI options
    share => 1	# Share the db connection with other Oak::Filer::DBI objects 
   )
    
  my $nome = $filer->load("nome");
  $filer->store(nome => lc($nome));

=head1 DESCRIPTION

This module provides access for saving data into a DBI table, to be used by
a Persistent descendant to save its data. Must pass table, prikey and privalue

=head1 OBJECT PROPERTIES

=over 4

=item datasource (readonly)

DBI datasorce string, used to create the connection and to share the connection
with other objects, defined using the parameters passed to new.

=item hostname,database,dbdriver,username,password,options

DBI options. See DBI documentation for more help.

=item table, prikey, privalue

Used to implement load, store and list.

=back

=head1 OBJECT METHODS

=over 4

=item constructor(PARAMS)

Called by new. You do not want do call it by yourself.
Iniciate the database connection (if not (shared and exists)), prepare to work
with determined table and register (setted by privalue).

=back

=cut

sub constructor {
	my $self = shift;
	my %params = @_;
	unless ($self->test_required_params(%params)) {
		return $self->call_exception
		  (
		   'fatal: errParams: Missing parameters creating '.ref($self)
		  );
	}
	$self->set		# Avoid inexistent properties
	  (
	   hostname => $params{hostname},
	   database => $params{database},
	   dbdriver => $params{dbdriver},
	   username => $params{username},
	   password => $params{password},
	   options => $params{options},
	   table => $params{table},
	   prikey => $params{prikey},
	   privalue => $params{privalue},
	   share => $params{share}
	  );
	return $self->register_connection;
}

=over 4

=item test_required_params(PARAMS)

Test if required params for the creation of the object exists. Called by constructor.

=back

=cut

sub test_required_params {
	my $self = shift;
	my %params = @_;
	return undef unless
	  (
	   $params{dbdriver} &&
	   $params{database} &&
	   $params{hostname}
	  );
	return 1;
}

=over

=item register_connection

Register the connection for this object, implements the shared connection. Called by
constructor.

=back

=cut

sub register_connection {
	my $self = shift;
	if ($self->get('share')) {
		unless ($Oak::Filer::DBI::Hash_Dbh_Refcount{$self->get('datasource')}{$self->get('username')}) {
			$Oak::Filer::DBI::Hash_Dbh{$self->get('datasource')}{$self->get('username')} ||= 
			  DBI->connect
			    (
			     $self->get('datasource'),
			     $self->get('username'),
			     $self->get('password'),
			     $self->get('options')
			    );
		}
		$Oak::Filer::DBI::Hash_Dbh_Refcount{$self->get('datasource')}{$self->get('username')}++;
		$self->{dbh} = $Oak::Filer::DBI::Hash_Dbh{$self->get('datasource')}{$self->get('username')};
	} else {
		$self->{dbh} =
		  DBI->connect
		    (
		     $self->get('datasource'),
		     $self->get('username'),
		     $self->get('password'),
		     $self->get('options')
		    );		  
	}
	unless ($self->{dbh}) {
		if ($self->get('share')) {
			$Oak::Filer::DBI::Hash_Dbh_Refcount{$self->get('datasource')}{$self->get('username')}--;
		}
		return $self->callException
		  (
		   'fatal: dbhFail: Error while trying to access db server with datasource '.$self->get('datasource')
		  );
	}
	return 1;
}

=over

=item load(FIELD,FIELD,...)

Loads one or more properties of the selected DBI table with the selected WHERE statement.
Returns a hash with the properties.

=back

=cut

sub load {
	my $self = shift;
	my $table = $self->get('table');
	my $where = $self->make_where_statement;
	return {} unless $table && $where;
	my @props = @_;
	my $fields = join(',',@props);
	my $sql = "SELECT $fields FROM $table WHERE $where";
	my $sth = $self->do_sql($sql);
	return () unless $sth->rows;
	return %{$sth->fetchrow_hashref};
}

=over

=item store(FIELD=>VALUE,FIELD=>VALUE,...)

Saves the data into the selected table with the selected WHERE statement.

=back

=cut

sub store {
	my $self = shift;	
	my $table = $self->get('table');
	my $where = $self->make_where_statement;
	return 0 unless $table && $where;
	my %args = @_;
	my @fields;
	foreach my $p (keys %args) {
		$args{$p} = $self->quote($args{$p});
		push @fields, "$p=$args{$p}"
	}
	my $set = join(',', @fields);
	my $sql = "UPDATE $table SET $set WHERE $where";
	$self->do_sql($sql);
	return 1;
}

=over

=item quote

Quotes a string, using DBI->quote unless empty, else uses "''".

=back

=cut

sub quote {
	my $self = shift;
	my $str = shift;
	unless (($str eq '') || (!defined $str)) {
		$str = $self->{dbh}->quote($str);
	} else {
		$str = "''";
	}
	return $str;
}

=over

=item make_where_statement

Returns the parameters for the where statement.

=back

=cut

sub make_where_statement {
	my $self = shift;
	my $where;
	my @fields;
	my $hr_where = $self->get('where');
	return 0 unless ref $hr_where;
	foreach my $w (keys %{$hr_where}) {
		push @fields, $w."=".$self->quote($hr_where->{$w});
	}
	return join(' AND ',@fields);
}

=over

=item do_sql(SQL)

Prepare, executes and test if successfull. Returns the Sth.

=back

=cut

sub do_sql {
	my $self = shift;
	my $sql = shift;
	my $sth = $self->{dbh}->prepare($sql);
	return $self->call_exception
	  (
	   'dbhFail: Syntax Error in Sql Expression ($sql)'
	  ) unless defined $sth;
	my $rv = $sth->execute;
	return $self->call_exception
	  (
	   'dbhFail: Error while executing sql ($sql)'
	  ) unless (defined $sth) and ($rv);
	return $sth;
}

# does not need documentation, this implementation is only used internally.
sub get_hash {
	my $self = shift;
	my @props = @_;
	my %retorno = $self->SUPER::getHash(@props);
	for (@props) {
		/^datasource$/ && do {
			$retorno{$_} = "DBI:".$self->get('dbdriver').":".$self->get('database')."@".$self->get('hostname');
			next;
		}
	}
	return %retorno;
}

=over

=item release_connection

Called by DESTROY, releases the DBI connection. Disconnect if not sharing, or if sharing and the last using.

=back

=cut

sub release_connection {
	my $self = shift;
	if ($self->get('share')) {
		$Oak::Filer::DBI::Hash_Dbh_Refcount{$self->get('datasource')}{$self->get('username')}--;
		unless ($Oak::Filer::DBI::Hash_Dbh_Refcount{$self->get('datasource')}{$self->get('username')}) {
			$self->{dbh}->disconnect if $self->{dbh};
		}
	} else {
		$self->{dbh}->disconnect if $self->{dbh};
	}	
	$self->{dbh} = undef;
	return 1;
}

sub DESTROY {
	my $self = shift;
	return $self->release_connection;
	$self->SUPER::DESTROY;
}

1;

__END__

=head1 BUGS

Too early to know...

=head1 COPYRIGHT

Copyright (c) 2001 Daniel Ruoso <daniel@ruoso.com>. All rights reserved.
This program is free software; you can redistribute it and/or
modify it under the same terms as Perl itself.

=cut
