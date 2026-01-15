// sudo dnf install fpc -y
//
// fpc bloom_filter_test.pas
//
// ./bloom_filter_test

program BloomFilterTest;

{$mode objfpc}{$H+}
{$codepage UTF8}

uses
  SysUtils, Math;

type
  TBloomFilter = record
    Bits: array of Byte;      // bit array stored as bytes
    Size: Cardinal;           // total number of bits
    HashCount: Byte;          // number of hash functions
    Count: Cardinal;          // number of items inserted
  end;

function CreateBloomFilter(bitSize: Cardinal; hashCount: Byte): TBloomFilter;
begin
  Result.Size := bitSize;
  Result.HashCount := hashCount;
  Result.Count := 0;
  
  // Round up to full bytes
  SetLength(Result.Bits, (bitSize + 7) div 8);
  FillChar(Result.Bits[0], Length(Result.Bits), 0);
end;

procedure DestroyBloomFilter(var bf: TBloomFilter);
begin
  SetLength(bf.Bits, 0);
  bf.Size := 0;
  bf.Count := 0;
end;

// Very simple but reasonably good hash functions for demonstration
function Hash1(const s: string; seed: Cardinal): Cardinal;
var
  i: Integer;
  h: Cardinal;
begin
  h := seed;
  for i := 1 to Length(s) do
    h := h * 31 + Ord(s[i]);
  Result := h;
end;

function Hash2(const s: string; seed: Cardinal): Cardinal;
var
  i: Integer;
  h: Cardinal;
begin
  h := seed xor $A5A5A5A5;
  for i := 1 to Length(s) do
    h := ((h shl 5) or (h shr 27)) xor Ord(s[i]);
  Result := h;
end;

// Get bit position (0..Size-1)
function GetBitPos(hash: Cardinal; bf: TBloomFilter): Cardinal;
begin
  Result := hash mod bf.Size;
end;

procedure SetBit(var bf: TBloomFilter; pos: Cardinal);
var
  byteIdx: Cardinal;
  bitIdx: Byte;
begin
  byteIdx := pos div 8;
  bitIdx := pos mod 8;
  bf.Bits[byteIdx] := bf.Bits[byteIdx] or (1 shl bitIdx);
end;

function TestBit(const bf: TBloomFilter; pos: Cardinal): Boolean;
var
  byteIdx: Cardinal;
  bitIdx: Byte;
begin
  byteIdx := pos div 8;
  bitIdx := pos mod 8;
  Result := (bf.Bits[byteIdx] and (1 shl bitIdx)) <> 0;
end;

procedure Insert(var bf: TBloomFilter; const item: string);
var
  i: Byte;
  h1, h2: Cardinal;
begin
  for i := 0 to bf.HashCount - 1 do
  begin
    h1 := Hash1(item, i * 17);
    h2 := Hash2(item, i * 31);
    SetBit(bf, GetBitPos(h1 xor h2, bf));
  end;
  Inc(bf.Count);
end;

function ProbablyContains(const bf: TBloomFilter; const item: string): Boolean;
var
  i: Byte;
  h1, h2: Cardinal;
begin
  Result := True;
  for i := 0 to bf.HashCount - 1 do
  begin
    h1 := Hash1(item, i * 17);
    h2 := Hash2(item, i * 31);
    if not TestBit(bf, GetBitPos(h1 xor h2, bf)) then
    begin
      Result := False;
      Exit;
    end;
  end;
end;

procedure RunTest;
const
  ITEMS_TO_INSERT = 100000;
  TEST_FALSE_POS  = 1000000;
var
  bf: TBloomFilter;
  i: Integer;
  falsePositives: Integer;
  item: string;
begin
  WriteLn('Creating Bloom Filter...');
  // ~1% false positive rate for 100k items
  bf := CreateBloomFilter(1200000, 7);  // ~1.2M bits, 7 hashes

  WriteLn('Inserting ', ITEMS_TO_INSERT, ' items...');
  for i := 1 to ITEMS_TO_INSERT do
  begin
    item := Format('test_item_%d', [i]);
    Insert(bf, item);
  end;

  WriteLn('Testing known existing items...');
  for i := 1 to 1000 do
  begin
    item := Format('test_item_%d', [i]);
    if not ProbablyContains(bf, item) then
      WriteLn('ERROR: False negative for: ', item);
  end;

  WriteLn('Testing non-existing items (false positive check)...');
  falsePositives := 0;
  for i := 1 to TEST_FALSE_POS do
  begin
    item := Format('fake_item_%d', [i]);
    if ProbablyContains(bf, item) then
      Inc(falsePositives);
  end;

  WriteLn('False positives: ', falsePositives, ' / ', TEST_FALSE_POS);
  WriteLn(Format('False positive rate: %.3f%%', 
    [100.0 * falsePositives / TEST_FALSE_POS]));

  WriteLn('Estimated theoretical false positive rate: ~', 
    Exp(-7 * 100000 / 1200000 * Ln(2)):0:4);

  DestroyBloomFilter(bf);
end;

begin
  try
    WriteLn('=== Bloom Filter Test Program ===');
    WriteLn('Free Pascal version: ', {$I %FPCVERSION%});
    WriteLn;
    
    RunTest;
    
    WriteLn;
    WriteLn('Test finished successfully.');
  except
    on E: Exception do
    begin
      WriteLn('ERROR: ', E.ClassName, ': ', E.Message);
      ExitCode := 1;
    end;
  end;
end.