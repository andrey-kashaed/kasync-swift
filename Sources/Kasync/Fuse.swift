//
// DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
//
// Copyright (c) 2023 Andrey Kashaed. All rights reserved.
//
// The contents of this file are subject to the terms of the
// Common Development and Distribution License 1.0 (the "License").
// You may not use this file except in compliance with the License.
//
// You can obtain a copy of the License at
// https://opensource.org/licenses/CDDL-1.0 or LICENSE.txt.
//

import Foundation

public class Fuse {
    
    public enum Backoff {
        case linear
        case fibonacci
        case exponential
    }
    
    private let backoff: Backoff
    private let factor: Double
    private let tryLimit: Int
    
    public init(backoff: Backoff, factor: Double, tryLimit: Int) {
        self.backoff = backoff
        self.factor = factor
        self.tryLimit = tryLimit
    }
    
    @discardableResult
    public func protected<T>(_ block: () async throws -> T) async rethrows -> T {
        var tryNumber = 0
        while true {
            do {
                tryNumber += 1
                return try await block()
            } catch {
                if error is CancellationError || tryNumber >= tryLimit {
                    throw error
                }
                let timeout = timeout(tryNumber: tryNumber)
                try await Task.sleep(nanoseconds: UInt64(timeout * 1000_000_000))
            }
        }
    }
    
    private func timeout(tryNumber: Int) -> Double {
        switch backoff {
        case .linear:
            return factor
        case .fibonacci:
            return factor * fibonacci(n: tryNumber)
        case .exponential:
            return factor * exponential(n: tryNumber)
        }
    }
   
    private func fibonacci(n: Int) -> Double {
        var a = 0
        var b = 1
        for _ in 0..<n {
            let temp = a
            a = b
            b = temp + b
        }
        return Double(a)
    }
    
    private func exponential(n: Int) -> Double {
        exp(Double(n))
    }
    
}
