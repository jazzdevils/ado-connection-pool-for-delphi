	You can use this unit like this.
 
 	uses ConnectionPoolUnit;   

    var
	  oQuery: TADOQuery;
	begin
	  oQuery := TADOQuery.Create(nil);
	  try
	    if oQuery.Active then
	      oQuery.Close;

	    oQuery.Connection := ConnectionPool.GetConnection.Connection;
	    oQuery.SQL.Text := 'select * from Items  where ID = '+ IntToStr(iID);

	    oQuery.Open;

	    Result := oQuery.RecordCount;
	  finally
	    oQuery.Free;
	  end;
	end;
