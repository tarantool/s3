Name: tarantool-s3
Version: 0.9.0
Release: 1%{?dist}
Summary: Tarantool s3 bindings
Group: Applications/Databases
License: BSD
URL: https://github.com/tarantool/s3
Source0: https://github.com/tarantool/s3/archive/%{version}/%{name}-%{version}.tar.gz
BuildRequires: cmake >= 2.8
BuildRequires: gcc >= 4.5
BuildRequires: tarantool-devel >= 1.6.8.0
BuildRequires: msgpuck-devel >= 1.0.0
BuildRequires: libcurl-devel
BuildRequires: openssl-devel
BuildRequires: libxml2-devel

BuildRequires: /usr/bin/prove
Requires: tarantool >= 1.6.8.0
Requires: libcurl-devel
Requires: openssl-devel
Requires: libxml2-devel

%description
Amazon s3 bindings for tarantool

%prep
%setup -q -n %{name}-%{version}

%build
%cmake . -DCMAKE_BUILD_TYPE=RelWithDebInfo
make %{?_smp_mflags}

%check
make %{?_smp_mflags} check

%install
%make_install

%files
%{_libdir}/tarantool/*/
%{_datarootdir}/tarantool/*/
%doc README.md
%{!?_licensedir:%global license %doc}
%license LICENSE AUTHORS

%changelog
* Mon Sep 26 2016 Andrey Drozdov <andrey@tarantool.org> 1.0.0-1
- Initial version of the RPM spec
