unit ConnectionPoolUnit;

interface

uses
  Windows, Classes, SysUtils, DateUtils, SyncObjs, DB, SqlExpr, ADODB;

type
  IConnection = Interface(IInterface)
    function Connection: TADOConnection;
    function GetRefCount: Integer;
    function GetLastAccess: TDateTime;
    property LastAccess: TDateTime read GetLastAccess;
    property RefCount: Integer read GetRefCount;
  end;

  TConnectionModule = class(TObject, IConnection)
  private
    ADOConnection: TADOConnection;

  protected
    FRefCount: Integer;
    FLastAccess: TDateTime;
    CriticalSection: TCriticalSection;
    Semaphore: THandle;
    
    function QueryInterface(const IID: TGUID; out Obj): HResult; stdcall;
    function _AddRef: Integer; stdcall;
    function _Release: Integer; stdcall;

    {IConnection methods}
    function GetLastAccess: TDateTime;
    function GetRefCount: Integer;
  public
    { Public declarations }

    constructor Create(HostName, DBName, ID, PassWord : String);
    destructor Destroy; Override;

    function Connection: TADOConnection;
  end;

  TCleanupThread = Class;

  TFixedConnectionPool = Class(TObject)
  private
    FPool: Array of IConnection;
    FPoolSize: Integer;
    FPoolCount: Integer;
    FConnectionIndex: Integer;
    FTimeout: LargeInt;
    CleanupThread: TCleanupThread;
    Semaphore: THandle;
    CriticalSection: TCriticalSection;

    FHostName, FDBName, FID, FPassWord: String; 
  public
    constructor Create(HostName, DBName, ID, PassWord : String;
      const PoolSize: Integer = 10;
      const CleanupDelayMinutes: Integer = 5;
      const Timeoutms: Integer = 10000); Overload;

    destructor Destroy; override;

    function GetConnection: IConnection;

    property Count: Integer read FPoolCount;
    property ConnIndex: Integer read FConnectionIndex;
  end;

  TCleanupThread = class(TThread)
  private
    FCleanupDelay: Integer;
  protected
    CriticalSection: TCriticalSection;
    FixedConnectionPool: TFixedConnectionPool;

    procedure Execute; override;
    constructor Create(CreateSuspended: Boolean; const CleanupDelayMinutes: Integer);
  end;

  EConnPoolException = class(Exception)
  public
    constructor Create(const Msg: string);
  end;

  function ConnectionPool: TFixedConnectionPool;
var
  ConnPool: TFixedConnectionPool;
  InternalEvent: TEvent;

implementation

{ TConnectionModule }

function ConnectionPool: TFixedConnectionPool;
begin
  if Not Assigned(ConnPool) then
    ConnPool := TFixedConnectionPool.Create('IP or Host', 'DB Name', 'User ID', 'Password', 10, 5, 20000);

  Result := ConnPool;
end;

function TConnectionModule.Connection: TADOConnection;
begin
  Result := ADOConnection;
end;

constructor TConnectionModule.Create(HostName, DBName, ID, PassWord : String);
var
  DBStr: String;
begin
  inherited Create;

  DBStr := Format('Provider=SQLOLEDB.1;Password=%s;Persist Security Info=True;User ID=%s;Data Source=%s;Initial Catalog=%s'
    , [PassWord, ID, HostName, DBName]);

  ADOConnection := TADOConnection.Create(nil);
  ADOConnection.KeepConnection := True;
  ADOConnection.ConnectionString := DBStr;
  ADOConnection.LoginPrompt := False;
  ADOConnection.ConnectionTimeout := 5;
  ADOConnection.CommandTimeout := 300;

  try
    ADOConnection.Open;
  except
  end;
end;

destructor TConnectionModule.Destroy;
begin
  ADOConnection.Close;
  ADOConnection.Free;
  ADOConnection := Nil;

  inherited;
end;

function TConnectionModule.GetLastAccess: TDateTime;
begin
  CriticalSection.Enter;
  try
    Result := FLastAccess;
  finally
    CriticalSection.Leave;
  end;
end;

function TConnectionModule.GetRefCount: Integer;
begin
  CriticalSection.Enter;
  try
    Result := FRefCount;
  finally
    CriticalSection.Leave;
  end;
end;

function TConnectionModule.QueryInterface(const IID: TGUID; out Obj): HResult;
begin
  if GetInterface(IID, Obj) then
    Result := 0
  else
    Result := E_NOINTERFACE;
end;

function TConnectionModule._AddRef: Integer;
begin
  CriticalSection.Enter;
  try
    Inc(FRefCount);
    Result := FRefCount;
  finally
    CriticalSection.Leave;
  end;
end;


function TConnectionModule._Release: Integer;
var
  tmpCriticalSection: TCriticalSection;
  tmpSemaphore: THandle;
begin
  tmpCriticalSection := CriticalSection;
  tmpSemaphore := Semaphore;

  Result := FRefCount;

  CriticalSection.Enter;
  try
    Dec(FRefCount);

    Result := FRefCount;

    if Result = 0 then
      Destroy
    else
      Self.FLastAccess := Now;
  finally
    tmpCriticalSection.Leave;

    if Result = 1 then
      ReleaseSemaphore(tmpSemaphore, 1, nil);
  end;
end;

{ TFixedConnectionPool }

constructor TFixedConnectionPool.Create(HostName, DBName, ID, PassWord : String;
  const PoolSize, CleanupDelayMinutes: Integer; const Timeoutms: Integer);
begin
  inherited Create;

  FHostName := HostName;
  FDBName := DBName;
  FID := ID;
  FPassWord := PassWord;

  FPoolSize := PoolSize;
  FTimeout := Timeoutms;
  Semaphore := CreateSemaphore(nil, PoolSize, PoolSize, '');
  CriticalSection := TCriticalSection.Create;

  SetLength(FPool, PoolSize);

  CleanupThread := TCleanupThread.Create(True, CleanupDelayMinutes);

  CleanupThread.FreeOnTerminate := True;
  CleanupThread.Priority := tpLower;
  CleanupThread.FixedConnectionPool := Self;
  CleanupThread.Resume;
end;

destructor TFixedConnectionPool.Destroy;
var
  i: Integer;
begin
  CleanupThread.Terminate;

  InternalEvent.SetEvent;

  CriticalSection.Enter;
  try
    for i := Low(FPool) to High(FPool) do
      FPool[i] := nil;
    SetLength(FPool,0);
  finally
    CriticalSection.Leave;
  end;

  CriticalSection.Free;
  CloseHandle(Semaphore);

  inherited;
end;


function TFixedConnectionPool.GetConnection: IConnection;
var
  i: Integer;
  DM: TConnectionModule;
  WaitResult: Integer;
begin
  Result := Nil;

  WaitResult := WaitForSingleObject(Semaphore, FTimeout);
  if WaitResult <> WAIT_OBJECT_0 then
    raise EConnPoolException.Create('Connection pool timeout. Cannot get a connection');

  CriticalSection.Enter;
  try
    for i := Low(FPool) to High(FPool) do begin
      if Not Assigned(FPool[i]) then begin
        DM := TConnectionModule.Create(FHostName, FDBName, FID, FPassWord);
        DM.CriticalSection := CriticalSection;
        DM.Semaphore := Semaphore;
        FPool[i] := DM;
        FPool[i].Connection.Connected := True;

        FPoolCount := i + 1;
        FConnectionIndex := i;
        
        Result := FPool[i];

//        Exit;
      end;

      if FPool[i].RefCount = 1 then begin
//        if FPool[i].Connection.State = [stOpen] then begin
        FConnectionIndex := i;

        Result := FPool[i];

        Break;
//        end;
      end;
    end;
  finally
    CriticalSection.Leave;
  end;
end;

{ TCleanupThread }

constructor TCleanupThread.Create(CreateSuspended: Boolean;
  const CleanupDelayMinutes: Integer);
begin
  inherited Create(True); 

  FCleanupDelay := CleanupDelayMinutes;

  if not CreateSuspended then
    Resume;
end;

procedure TCleanupThread.Execute;
var
  i: Integer;
  WaitMinutes: Integer;
begin
  WaitMinutes := FCleanupDelay * 1000 * 60;
  while True do begin
    if Terminated then
      Exit;

    if InternalEvent.WaitFor(WaitMinutes) <> wrTimeout then
      Exit;

    if Terminated then
      Exit;

    FixedConnectionPool.CriticalSection.Enter;
    try
      for i := low(FixedConnectionPool.FPool) to High(FixedConnectionPool.FPool) do begin
        if (FixedConnectionPool.FPool[i] <> nil) then
          if (FixedConnectionPool.FPool[i].RefCount = 1) then
            if (MinutesBetween(FixedConnectionPool.FPool[i].LastAccess, Now) > FCleanupDelay) then
              FixedConnectionPool.FPool[i] := nil;
      end;
    finally
      FixedConnectionPool.CriticalSection.Leave;
    end;
  end;
end;

{ EConnPoolException }

constructor EConnPoolException.Create(const Msg: string);
begin
  inherited Create(Msg);
end;

initialization
  InternalEvent := TEvent.Create(nil, False, False, '');

finalization
  InternalEvent.Free;

end.
//ConnPool := TFixedConnectionPool.Create('HostName', 'DBName', 'ID', 'Password', 10, 5, 20000);
//ConnPool.Free


